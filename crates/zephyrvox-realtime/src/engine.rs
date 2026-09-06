use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::{
    ClientEvent, ConnectionStatus, RealtimeError, StateApplyError, SyncMachine, WebSocketMessage,
    connection::ConnectionInner,
    frame::{ServerFrame, encode_sync_hello, parse_server_frame},
};

/// Runs the reconnecting owner task for one public connection handle.
pub(crate) async fn run(inner: Arc<ConnectionInner>) {
    let policy = inner.config.reconnect_policy();
    let mut attempt = 0_usize;
    loop {
        if is_stopped(&inner) {
            break;
        }
        set_status(
            &inner,
            if attempt == 0 {
                ConnectionStatus::Connecting
            } else {
                ConnectionStatus::Reconnecting
            },
        );

        let access_token = match load_access_token(&inner).await {
            Ok(token) => token,
            Err(error) => {
                if terminal_error(&error) || !policy.enabled() {
                    break;
                }
                if !wait_for_retry(&inner, policy, attempt).await {
                    break;
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let result = match inner
            .connector
            .connect(inner.websocket_url.clone(), access_token)
            .await
        {
            Ok(session) => futures_lite_run_socket(Arc::clone(&inner), session).await,
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            let _ = inner.event_tx.send(ClientEvent::Disconnected {
                reason: error.to_string(),
            });
        }
        clear_socket_state(&inner).await;
        if is_stopped(&inner) || matches!(result, Ok(())) {
            break;
        }
        let error = result.expect_err("socket result was checked as non-success");
        if terminal_error(&error) || !policy.enabled() {
            break;
        }
        if !wait_for_retry(&inner, policy, attempt).await {
            break;
        }
        attempt = attempt.saturating_add(1);
    }
    clear_socket_state(&inner).await;
    set_status(&inner, ConnectionStatus::Closed);
}

/// Keeps the engine source independent from a concrete executor helper name.
/// Owns one socket generation until shutdown, EOF, or a terminal protocol
/// failure. All sends and receives for that generation stay on this task.
async fn futures_lite_run_socket(
    inner: Arc<ConnectionInner>,
    mut session: Box<dyn crate::WebSocketSession>,
) -> Result<(), RealtimeError> {
    let mut machine = SyncMachine::new(&*inner.state.snapshot().await);
    machine.begin_connecting()?;

    let first = timeout(Duration::from_secs(10), session.receive())
        .await
        .map_err(|_| RealtimeError::Transport("connection.ready timeout".to_owned()))??;
    let ServerFrame::ConnectionReady(ready) = text_frame(first)? else {
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
    set_status(&inner, ConnectionStatus::Ready);
    let _ = inner.event_tx.send(ClientEvent::Ready {
        control_connection_id: ready.control_connection_id,
        access_expires_at: ready.access_expires_at,
    });

    let cursor = machine.begin_sync()?;
    session.send_text(encode_sync_hello(&cursor)?).await?;
    set_status(&inner, ConnectionStatus::Syncing);

    let (outbound, mut outbound_rx) = mpsc::channel(inner.config.maximum_pending_commands() + 4);
    *inner.outbound.lock().await = Some(outbound);
    let mut stop_rx = inner.stop_tx.subscribe();

    loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_ok() && *stop_rx.borrow() {
                    let _ = session.close().await;
                    return Ok(());
                }
            }
            outgoing = outbound_rx.recv() => {
                match outgoing {
                    Some(text) => session.send_text(text).await?,
                    None => return Err(RealtimeError::Closed),
                }
            }
            incoming = timeout(Duration::from_secs(90), session.receive()) => {
                let incoming = incoming
                    .map_err(|_| RealtimeError::Transport("WebSocket liveness timeout".to_owned()))??;
                let Some(incoming) = incoming else {
                    return Err(RealtimeError::Transport("WebSocket peer closed".to_owned()));
                };
                if process_message(&inner, &mut machine, &mut *session, incoming).await? {
                    return Ok(());
                }
            }
        }
    }
}

/// Processes one frame and returns `true` only when the socket should stop
/// normally. A `sync.required` path stays on the same socket after replacing
/// the HTTP snapshot.
/// Converts one transport message into a protocol frame or a ping response.
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
        WebSocketMessage::Close(reason) => {
            return Err(RealtimeError::Transport(format!(
                "WebSocket closed{}",
                reason
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default()
            )));
        }
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

/// Applies one parsed protocol frame to the synchronization and command state.
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
            machine.accept_replay(&replay)?;
            for event in replay.events {
                if !apply_or_resync(inner, machine, session, event).await? {
                    break;
                }
            }
            Ok(())
        }
        ServerFrame::SyncComplete(complete) => {
            let queued = machine.accept_complete(&complete)?;
            inner.state.update_cursor(complete.cursor).await;
            set_status(inner, ConnectionStatus::Live);
            for event in queued {
                if !apply_or_resync(inner, machine, session, event).await? {
                    break;
                }
            }
            Ok(())
        }
        ServerFrame::SyncRequired(required) => {
            let _ = inner.event_tx.send(ClientEvent::SyncRequired {
                reason: required.reason.clone(),
            });
            replace_snapshot_and_hello(inner, machine, session, required.reason).await
        }
        ServerFrame::StateEvent(event) => {
            if let Some(event) = machine.accept_live_event(event)? {
                let _ = apply_or_resync(inner, machine, session, event).await?;
            }
            Ok(())
        }
        ServerFrame::CommandOk(ack) => {
            if let Some(expires_at) = ack.expires_at {
                inner.access_expires_at.store(expires_at, Ordering::Release);
            }
            resolve_pending(inner, ack.request_id.clone(), Ok(ack)).await;
            Ok(())
        }
        ServerFrame::CommandError(error) => {
            let request_id = error.request_id.clone();
            resolve_pending(
                inner,
                request_id,
                Err(crate::connection::command_failure(error)),
            )
            .await;
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
    inner.state.replace_snapshot(snapshot).await;
    let current = inner.state.snapshot().await;
    machine.replace_snapshot(&current);
    let cursor = machine.begin_sync()?;
    session.send_text(encode_sync_hello(&cursor)?).await
}

/// Completes exactly one request waiter without depending on response order.
async fn resolve_pending(
    inner: &Arc<ConnectionInner>,
    request_id: String,
    result: Result<crate::CommandAck, RealtimeError>,
) {
    if let Some(sender) = inner.pending.lock().await.remove(&request_id) {
        let _ = sender.send(result);
    }
}

/// Clears generation-owned identity and completes all waiters on socket loss.
async fn clear_socket_state(inner: &Arc<ConnectionInner>) {
    *inner.outbound.lock().await = None;
    if let Ok(mut control_id) = inner.control_id.write() {
        *control_id = None;
    }
    inner.access_expires_at.store(0, Ordering::Release);
    let mut pending = inner.pending.lock().await;
    for (_, sender) in pending.drain() {
        let _ = sender.send(Err(RealtimeError::Closed));
    }
}

/// Loads the next access lease from the injected provider.
async fn load_access_token(
    inner: &Arc<ConnectionInner>,
) -> Result<crate::AccessToken, RealtimeError> {
    inner.provider.access_token().await
}

/// Waits for bounded exponential backoff unless the host has cancelled.
async fn wait_for_retry(
    inner: &Arc<ConnectionInner>,
    policy: crate::ReconnectPolicy,
    attempt: usize,
) -> bool {
    if policy
        .maximum_attempts()
        .is_some_and(|maximum| attempt >= maximum)
    {
        return false;
    }
    let delay = policy.delay(attempt);
    let mut stop = inner.stop_tx.subscribe();
    set_status(inner, ConnectionStatus::Reconnecting);
    tokio::select! {
        _ = tokio::time::sleep(delay) => !is_stopped(inner),
        changed = stop.changed() => changed.is_err() || !*stop.borrow(),
    }
}

/// Requires the first socket message to be the text `connection.ready` frame.
fn text_frame(message: Option<WebSocketMessage>) -> Result<ServerFrame, RealtimeError> {
    match message {
        Some(WebSocketMessage::Text(payload)) => parse_server_frame(&payload),
        Some(WebSocketMessage::Ping(_)) | Some(WebSocketMessage::Pong(_)) => Err(
            RealtimeError::Protocol("connection.ready must be a text frame".to_owned()),
        ),
        Some(WebSocketMessage::Close(reason)) => Err(RealtimeError::Transport(format!(
            "WebSocket closed before ready{}",
            reason
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default()
        ))),
        None => Err(RealtimeError::Transport(
            "WebSocket peer closed before ready".to_owned(),
        )),
    }
}

/// Classifies errors that must stop rather than enter automatic reconnect.
fn terminal_error(error: &RealtimeError) -> bool {
    matches!(
        error,
        RealtimeError::Closed
            | RealtimeError::Revoked(_)
            | RealtimeError::Protocol(_)
            | RealtimeError::Json(_)
            | RealtimeError::Sync(_)
            | RealtimeError::Configuration(_)
            | RealtimeError::AuthenticationRequired
            | RealtimeError::AuthenticationExpired
            | RealtimeError::TlsPinMismatch { .. }
            | RealtimeError::TlsCertificateInvalid
    )
}

/// Reads the host-owned shutdown flag without taking an async lock.
fn is_stopped(inner: &ConnectionInner) -> bool {
    inner.stopped.load(Ordering::Acquire)
}

/// Publishes a lifecycle status to both the watcher and combined event stream.
fn set_status(inner: &ConnectionInner, status: ConnectionStatus) {
    let _ = inner.status_tx.send(status);
    let _ = inner.event_tx.send(ClientEvent::StatusChanged(status));
}
