use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    net::{UdpSocket, lookup_host},
    sync::{broadcast, mpsc, oneshot, watch},
    time::{self, MissedTickBehavior},
};

use crate::{
    PacketCodec, PacketControl, RevocationReason, SessionKeyCache, VoiceError, VoiceStatus,
    replay::ReplayWindow,
};
use bytes::Bytes;
use zephyrvox_types::{InboundMedia, StreamTypeId, VoiceEndpoint, VoiceSessionId};
use zephyrvox_wire::MAX_PACKET_SIZE;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const RECEIVE_BUFFER_SIZE: usize = MAX_PACKET_SIZE + 256;

/// Shared state retained by the public session handle and its worker task.
pub(crate) struct SessionInner {
    pub(crate) session_id: VoiceSessionId,
    pub(crate) endpoint: VoiceEndpoint,
    pub(crate) expires_at: i64,
    pub(crate) encrypted: bool,
    pub(crate) max_payload: usize,
    pub(crate) status_tx: watch::Sender<VoiceStatus>,
    pub(crate) media_tx: broadcast::Sender<InboundMedia>,
    pub(crate) command_tx: mpsc::Sender<SessionCommand>,
    pub(crate) stop_tx: watch::Sender<Option<Termination>>,
    pub(crate) key_cache: Option<SessionKeyCache>,
}

/// One bounded command sent from a public handle to the UDP worker.
pub(crate) enum SessionCommand {
    /// Encoded media awaiting sequence allocation and a UDP write.
    Send {
        /// Registered media stream type.
        stream_type: StreamTypeId,
        /// Owned encoded codec payload.
        payload: Bytes,
        /// Completion channel for the UDP write.
        response: oneshot::Sender<Result<(), VoiceError>>,
    },
}

/// A lifecycle stop request delivered to the worker through a watch channel.
#[derive(Clone, Copy)]
pub(crate) enum Termination {
    /// Host-requested orderly close.
    Close,
    /// Non-revocation control or transport disconnect.
    Disconnect,
    /// Explicit server/control-plane revocation.
    Revoke(RevocationReason),
}

/// Resolves an advertised endpoint and connects an ephemeral local UDP socket.
pub(crate) async fn connect_socket(endpoint: &VoiceEndpoint) -> Result<UdpSocket, VoiceError> {
    let addresses = lookup_host((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(|error| VoiceError::Endpoint(error.to_string()))?;
    let mut last_error = None;
    for remote in addresses {
        let local = if remote.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = match UdpSocket::bind(local).await {
            Ok(socket) => socket,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        match socket.connect(remote).await {
            Ok(()) => return Ok(socket),
            Err(error) => last_error = Some(error),
        }
    }
    Err(VoiceError::Endpoint(
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "voice endpoint resolved to no addresses".to_owned()),
    ))
}

/// Runs one socket generation until close, revocation, receive failure, or
/// task cancellation. The worker owns all sequence and socket mutation.
pub(crate) async fn run(
    inner: Arc<SessionInner>,
    socket: UdpSocket,
    codec: PacketCodec,
    mut command_rx: mpsc::Receiver<SessionCommand>,
    mut stop_rx: watch::Receiver<Option<Termination>>,
    started_tx: oneshot::Sender<Result<(), VoiceError>>,
) {
    let mut transport_sequence = 1_u64;
    let mut channel_sequences = HashMap::new();
    let mut replay = ReplayWindow::default();
    let mut last_outbound = Instant::now();
    if let Err(error) =
        send_heartbeat(&socket, &codec, &mut transport_sequence, &mut last_outbound).await
    {
        remove_cached_key(&inner);
        publish_status(&inner, VoiceStatus::Disconnected);
        let _ = started_tx.send(Err(error));
        return;
    }
    publish_status(&inner, VoiceStatus::Active);
    let _ = started_tx.send(Ok(()));

    let heartbeat_start = time::Instant::now() + HEARTBEAT_INTERVAL;
    let mut heartbeat = time::interval_at(heartbeat_start, HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut buffer = vec![0_u8; RECEIVE_BUFFER_SIZE];

    loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_err() {
                    finish(&inner, VoiceStatus::Disconnected);
                    return;
                }
                if let Some(termination) = *stop_rx.borrow() {
                    let status = match termination {
                        Termination::Close | Termination::Disconnect => VoiceStatus::Disconnected,
                        Termination::Revoke(reason) => VoiceStatus::Revoked(reason),
                    };
                    finish(&inner, status);
                    return;
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    finish(&inner, VoiceStatus::Disconnected);
                    return;
                };
                if !handle_command(
                    &inner,
                    &socket,
                    &codec,
                    &mut transport_sequence,
                    &mut channel_sequences,
                    &mut last_outbound,
                    command,
                ).await {
                    return;
                }
            }
            received = socket.recv(&mut buffer) => {
                match received {
                    Ok(length) => {
                        if !handle_inbound(&inner, &codec, &mut replay, &buffer[..length]) {
                            return;
                        }
                    }
                    Err(_) => {
                        finish(&inner, VoiceStatus::Disconnected);
                        return;
                    }
                }
            }
            _ = heartbeat.tick() => {
                if last_outbound.elapsed() >= HEARTBEAT_INTERVAL
                    && send_heartbeat(
                        &socket,
                        &codec,
                        &mut transport_sequence,
                        &mut last_outbound,
                    ).await.is_err()
                {
                    finish(&inner, VoiceStatus::Disconnected);
                    return;
                }
            }
        }
    }
}

/// Executes one queued media command and reports whether the worker can live.
async fn handle_command(
    inner: &SessionInner,
    socket: &UdpSocket,
    codec: &PacketCodec,
    transport_sequence: &mut u64,
    channel_sequences: &mut HashMap<u8, u16>,
    last_outbound: &mut Instant,
    command: SessionCommand,
) -> bool {
    match command {
        SessionCommand::Send {
            stream_type,
            payload,
            response,
        } => {
            let result = send_media(
                socket,
                codec,
                transport_sequence,
                channel_sequences,
                last_outbound,
                stream_type,
                &payload,
            )
            .await;
            let terminal = matches!(
                result,
                Err(VoiceError::Transport(_)) | Err(VoiceError::SequenceExhausted)
            );
            let _ = response.send(result);
            if terminal {
                finish(inner, VoiceStatus::Disconnected);
                return false;
            }
        }
    }
    true
}

/// Validates, sequences, encodes, and writes one client media packet.
async fn send_media(
    socket: &UdpSocket,
    codec: &PacketCodec,
    transport_sequence: &mut u64,
    channel_sequences: &mut HashMap<u8, u16>,
    last_outbound: &mut Instant,
    stream_type: StreamTypeId,
    payload: &[u8],
) -> Result<(), VoiceError> {
    codec.validate_media(stream_type, payload.len())?;
    let channel_sequence = *channel_sequences.entry(stream_type.get()).or_insert(0);
    let sequence = reserve_transport_sequence(transport_sequence)?;
    let packet = codec.encode_media(sequence, channel_sequence, stream_type, payload)?;
    socket.send(&packet).await.map_err(VoiceError::Transport)?;
    channel_sequences.insert(stream_type.get(), channel_sequence.wrapping_add(1));
    *last_outbound = Instant::now();
    Ok(())
}

/// Writes one empty heartbeat and advances the client transport sequence.
async fn send_heartbeat(
    socket: &UdpSocket,
    codec: &PacketCodec,
    transport_sequence: &mut u64,
    last_outbound: &mut Instant,
) -> Result<(), VoiceError> {
    let sequence = reserve_transport_sequence(transport_sequence)?;
    let packet = codec.encode_heartbeat(sequence)?;
    socket.send(&packet).await.map_err(VoiceError::Transport)?;
    *last_outbound = Instant::now();
    Ok(())
}

/// Reserves one non-wrapping transport sequence number.
fn reserve_transport_sequence(sequence: &mut u64) -> Result<u64, VoiceError> {
    if *sequence == u64::MAX {
        return Err(VoiceError::SequenceExhausted);
    }
    let current = *sequence;
    *sequence += 1;
    Ok(current)
}

/// Authenticates, replay-checks, and projects one received datagram.
///
/// Returns `false` after a revocation because the worker must not continue to
/// service sends or heartbeats once the server has withdrawn voice authority.
fn handle_inbound(
    inner: &SessionInner,
    codec: &PacketCodec,
    replay: &mut ReplayWindow,
    datagram: &[u8],
) -> bool {
    let packet = match codec.decode_server(datagram) {
        Ok(packet) => packet,
        Err(_) => return true,
    };
    if !replay.accept(packet.transport_seq) {
        return true;
    }
    match packet.control {
        Some(PacketControl::Heartbeat) => true,
        Some(PacketControl::Revocation(reason)) => {
            finish(inner, VoiceStatus::Revoked(reason));
            false
        }
        None => {
            let Some(speaker_id) = packet.speaker_id else {
                return true;
            };
            let _ = inner.media_tx.send(InboundMedia {
                speaker_id,
                stream_type: packet.stream_type,
                channel_seq: packet.channel_seq,
                transport_seq: packet.transport_seq,
                payload: packet.payload,
            });
            true
        }
    }
}

/// Publishes a status even when there are currently no watch subscribers.
pub(crate) fn publish_status(inner: &SessionInner, status: VoiceStatus) {
    if *inner.status_tx.borrow() != status {
        inner.status_tx.send_replace(status);
    }
}

/// Marks a worker terminal and removes its cached session key.
fn finish(inner: &SessionInner, status: VoiceStatus) {
    remove_cached_key(inner);
    publish_status(inner, status);
}

/// Removes the key for a session after any terminal lifecycle transition.
fn remove_cached_key(inner: &SessionInner) {
    if let Some(cache) = &inner.key_cache {
        let _ = cache.remove(inner.session_id);
    }
}
