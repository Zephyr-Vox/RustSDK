use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;

use super::{is_stopped, resolve_pending, set_status};
use crate::{
    ClientEvent, ConnectionStatus, RealtimeError, StateApplyError, SyncError, SyncMachine,
    WebSocketMessage,
    connection::ConnectionInner,
    frame::{ServerFrame, encode_sync_hello, parse_server_frame},
};

/// Runs one authenticated socket generation until shutdown, EOF, or a
/// terminal protocol failure. All sends and receives for that generation stay
/// on this task.
pub(super) async fn run(
    inner: Arc<ConnectionInner>,
    mut session: Box<dyn crate::WebSocketSession>,
) -> Result<(), RealtimeError> {
    let mut machine = SyncMachine::new(&*inner.state.snapshot().await);
    machine.begin_connecting()?;
    let mut stop_rx = inner.stop_tx.subscribe();
    if is_stopped(&inner) {
        close_session(&mut *session).await;
        return Ok(());
    }

    let first = tokio::select! {
        incoming = timeout(Duration::from_secs(10), session.receive()) => incoming
            .map_err(|_| RealtimeError::Transport("connection.ready timeout".to_owned()))??,
        changed = stop_rx.changed() => {
            if changed.is_ok() && is_stopped(&inner) {
                close_session(&mut *session).await;
                return Ok(());
            }
            return Err(RealtimeError::Closed);
        }
    };
    let ServerFrame::ConnectionReady(ready) =
        text_frame(first, inner.config.maximum_frame_bytes())?
    else {
        return Err(RealtimeError::Protocol(
            "connection.ready must be the first server frame".to_owned(),
        ));
    };
    machine.accept_ready(ready.control_connection_id)?;
    if let Ok(mut control_id) = inner.control_id.write() {
        *control_id = Some(ready.control_connection_id);
    }
    inner
        .access_expires_at
        .store(ready.access_expires_at, Ordering::Release);
    let (outbound, mut outbound_rx) = mpsc::channel(inner.config.maximum_pending_commands() + 4);
    *inner.outbound.lock().await = Some(outbound);
    set_status(&inner, ConnectionStatus::Ready);
    let _ = inner.event_tx.send(ClientEvent::Ready {
        control_connection_id: ready.control_connection_id,
        access_expires_at: ready.access_expires_at,
    });

    let cursor = machine.begin_sync()?;
    let hello = encode_sync_hello(&cursor)?;
    tokio::select! {
        result = session.send_text(hello) => result?,
        changed = stop_rx.changed() => {
            if changed.is_ok() && is_stopped(&inner) {
                close_session(&mut *session).await;
                return Ok(());
            }
            return Err(RealtimeError::Closed);
        }
    }
    set_status(&inner, ConnectionStatus::Syncing);

    loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_ok() && *stop_rx.borrow() {
                    close_session(&mut *session).await;
                    return Ok(());
                }
                return Err(RealtimeError::Closed);
            }
            outgoing = outbound_rx.recv() => {
                match outgoing {
                    Some(text) => {
                        tokio::select! {
                            result = session.send_text(text) => result?,
                            changed = stop_rx.changed() => {
                                if changed.is_ok() && is_stopped(&inner) {
                                    close_session(&mut *session).await;
                                    return Ok(());
                                }
                                return Err(RealtimeError::Closed);
                            }
                        }
                    }
                    None => return Err(RealtimeError::Closed),
                }
            }
            incoming = timeout(Duration::from_secs(60), session.receive()) => {
                let incoming = incoming
                    .map_err(|_| RealtimeError::Transport("WebSocket liveness timeout".to_owned()))??;
                let Some(incoming) = incoming else {
                    return Err(RealtimeError::Transport("WebSocket peer closed".to_owned()));
                };
                let processed = tokio::select! {
                    result = process_message(&inner, &mut machine, &mut *session, incoming) => result?,
                    changed = stop_rx.changed() => {
                        if changed.is_ok() && is_stopped(&inner) {
                            close_session(&mut *session).await;
                            return Ok(());
                        }
                        return Err(RealtimeError::Closed);
                    }
                };
                if processed {
                    return Ok(());
                }
            }
        }
    }
}

/// Processes one transport message and returns `true` only when the socket
/// should stop normally.
async fn process_message(
    inner: &Arc<ConnectionInner>,
    machine: &mut SyncMachine,
    session: &mut dyn crate::WebSocketSession,
    message: WebSocketMessage,
) -> Result<bool, RealtimeError> {
    match message {
        WebSocketMessage::Ping(payload) => {
            session.send_pong(payload).await?;
        }
        WebSocketMessage::Pong(_) => {}
        WebSocketMessage::Close(reason) => return Err(close_error(None, reason)),
        WebSocketMessage::CloseFrame { code, reason } => return Err(close_error(code, reason)),
        WebSocketMessage::Text(payload) => {
            if payload.len() > inner.config.maximum_frame_bytes() {
                return Err(RealtimeError::Protocol(
                    "inbound WebSocket frame exceeds configured limit".to_owned(),
                ));
            }
            process_frame(inner, machine, session, parse_server_frame(&payload)?).await?;
        }
    }
    Ok(false)
}

/// Applies one parsed protocol frame to synchronization and command state.
async fn process_frame(
    inner: &Arc<ConnectionInner>,
    machine: &mut SyncMachine,
    session: &mut dyn crate::WebSocketSession,
    frame: ServerFrame,
) -> Result<(), RealtimeError> {
    match frame {
        ServerFrame::ConnectionReady(_) => Err(RealtimeError::Protocol(
            "duplicate connection.ready".to_owned(),
        )),
        ServerFrame::SyncReplay(replay) => {
            if let Err(error) = machine.accept_replay(&replay) {
                return recover_sync_error(inner, machine, session, error).await;
            }
            for event in replay.events {
                if !apply_or_resync(inner, machine, session, event).await? {
                    break;
                }
            }
            Ok(())
        }
        ServerFrame::SyncComplete(complete) => {
            let queued = match machine.accept_complete(&complete) {
                Ok(queued) => queued,
                Err(error) => return recover_sync_error(inner, machine, session, error).await,
            };
            inner.state.update_cursor(complete.cursor).await;
            for event in queued {
                if !apply_or_resync(inner, machine, session, event).await? {
                    return Ok(());
                }
            }
            // `wait_until_live` is also the host's publication barrier. Do
            // not advertise Live until all events that arrived during replay
            // have been installed in the state projection.
            set_status(inner, ConnectionStatus::Live);
            Ok(())
        }
        ServerFrame::SyncRequired(required) => {
            let _ = inner.event_tx.send(ClientEvent::SyncRequired {
                reason: required.reason.clone(),
            });
            replace_snapshot_and_hello(inner, machine, session, required.reason).await
        }
        ServerFrame::StateEvent(event) => {
            let event = match machine.accept_live_event(event) {
                Ok(event) => event,
                Err(error) => return recover_sync_error(inner, machine, session, error).await,
            };
            if let Some(event) = event {
                let _ = apply_or_resync(inner, machine, session, event).await?;
            }
            Ok(())
        }
        ServerFrame::CommandOk(ack) => {
            if let Some(expires_at) = ack.expires_at {
                inner.access_expires_at.store(expires_at, Ordering::Release);
            }
            resolve_pending(inner, ack.request_id.clone(), Ok(ack));
            Ok(())
        }
        ServerFrame::CommandError(error) => {
            let request_id = error.request_id.clone();
            resolve_pending(
                inner,
                request_id,
                Err(crate::connection::command_failure(error)),
            );
            Ok(())
        }
        ServerFrame::AuthRevoked(reason) => Err(RealtimeError::Revoked(reason.reason)),
        ServerFrame::Unknown(frame) if frame.frame_type.starts_with("state.") => {
            let reason = frame.frame_type;
            let _ = inner.event_tx.send(ClientEvent::SyncRequired {
                reason: reason.clone(),
            });
            replace_snapshot_and_hello(inner, machine, session, reason).await
        }
        ServerFrame::Unknown(frame) => {
            let _ = inner.event_tx.send(ClientEvent::Unknown(frame));
            Ok(())
        }
    }
}

/// Converts recoverable stream-order failures into a fresh snapshot handoff.
/// Invalid lifecycle transitions remain protocol failures because retrying
/// them on the same snapshot could hide a broken server state machine.
async fn recover_sync_error(
    inner: &Arc<ConnectionInner>,
    machine: &mut SyncMachine,
    session: &mut dyn crate::WebSocketSession,
    error: SyncError,
) -> Result<(), RealtimeError> {
    if !matches!(
        error,
        SyncError::ReplayBounds
            | SyncError::QueueLimit
            | SyncError::GeidOrder { .. }
            | SyncError::GeidGap { .. }
            | SyncError::StreamEpochChanged { .. }
            | SyncError::SnapshotRequired(_)
    ) {
        return Err(RealtimeError::Sync(error));
    }
    let reason = format!("state synchronization requires snapshot: {error}");
    let _ = inner.event_tx.send(ClientEvent::SyncRequired {
        reason: reason.clone(),
    });
    replace_snapshot_and_hello(inner, machine, session, reason).await
}

/// Applies one event and falls back to an HTTP snapshot when projection
/// validation discovers an unknown or malformed state evolution.
async fn apply_or_resync(
    inner: &Arc<ConnectionInner>,
    machine: &mut SyncMachine,
    session: &mut dyn crate::WebSocketSession,
    event: zephyrvox_types::StateEvent,
) -> Result<bool, RealtimeError> {
    let published = event.clone();
    match inner.state.apply_event(event).await {
        Ok(_) => {
            let _ = inner.event_tx.send(ClientEvent::State(published));
            Ok(true)
        }
        Err(error) => {
            let reason = match &error {
                StateApplyError::UnknownEvent(event) => format!("unknown state event {event}"),
                StateApplyError::SnapshotRequired(event) => {
                    format!("state event requires snapshot {event}")
                }
                other => format!("state projection rejected event: {other}"),
            };
            let _ = inner.event_tx.send(ClientEvent::SyncRequired {
                reason: reason.clone(),
            });
            replace_snapshot_and_hello(inner, machine, session, reason).await?;
            Ok(false)
        }
    }
}

/// Installs a fresh provider snapshot and starts a new hello on the same
/// authenticated socket.
async fn replace_snapshot_and_hello(
    inner: &Arc<ConnectionInner>,
    machine: &mut SyncMachine,
    session: &mut dyn crate::WebSocketSession,
    reason: String,
) -> Result<(), RealtimeError> {
    machine
        .require_snapshot(reason)
        .map_err(RealtimeError::Sync)?;
    set_status(inner, ConnectionStatus::Syncing);
    let snapshot = inner.provider.snapshot().await?;
    if let Some(authority) = inner.state.replace_snapshot(snapshot).await {
        let _ = inner.event_tx.send(ClientEvent::VoiceLost { authority });
    }
    let current = inner.state.snapshot().await;
    machine.replace_snapshot(&current);
    let cursor = machine.begin_sync()?;
    session.send_text(encode_sync_hello(&cursor)?).await
}

/// Closes a socket with a bounded wait so host shutdown cannot be held by a
/// transport that stopped servicing writes.
async fn close_session(session: &mut dyn crate::WebSocketSession) {
    let _ = timeout(Duration::from_secs(1), session.close()).await;
}

/// Requires the first socket message to be the text `connection.ready` frame.
fn text_frame(
    message: Option<WebSocketMessage>,
    maximum_frame_bytes: usize,
) -> Result<ServerFrame, RealtimeError> {
    match message {
        Some(WebSocketMessage::Text(payload)) => {
            if payload.len() > maximum_frame_bytes {
                return Err(RealtimeError::Protocol(
                    "inbound WebSocket frame exceeds configured limit".to_owned(),
                ));
            }
            parse_server_frame(&payload)
        }
        Some(WebSocketMessage::Ping(_)) | Some(WebSocketMessage::Pong(_)) => Err(
            RealtimeError::Protocol("connection.ready must be a text frame".to_owned()),
        ),
        Some(WebSocketMessage::Close(reason)) => Err(close_error(None, reason)),
        Some(WebSocketMessage::CloseFrame { code, reason }) => Err(close_error(code, reason)),
        None => Err(RealtimeError::Transport(
            "WebSocket peer closed before ready".to_owned(),
        )),
    }
}

/// Maps the server's close codes to terminal or reconnectable failures.
fn close_error(code: Option<u16>, reason: Option<String>) -> RealtimeError {
    let detail = || {
        reason
            .clone()
            .map(|reason| format!(": {reason}"))
            .unwrap_or_default()
    };
    match code {
        Some(4001) => RealtimeError::AuthenticationExpired,
        Some(4002) => RealtimeError::Revoked(
            reason.unwrap_or_else(|| "server revoked the connection".to_owned()),
        ),
        Some(4000 | 4003 | 4005) => RealtimeError::Protocol(format!(
            "WebSocket closed with terminal code {code:?}{}",
            detail()
        )),
        _ if reason
            .as_deref()
            .is_some_and(|reason| reason.eq_ignore_ascii_case("unauthorized")) =>
        {
            RealtimeError::AuthenticationExpired
        }
        _ => RealtimeError::Transport(format!("WebSocket closed{}", detail())),
    }
}
