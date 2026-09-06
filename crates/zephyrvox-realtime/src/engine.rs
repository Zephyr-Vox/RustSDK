use std::sync::{Arc, atomic::Ordering};

use crate::{ClientEvent, ConnectionStatus, RealtimeError, connection::ConnectionInner};

mod socket;

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

        let result = match connect_socket(&inner, access_token).await {
            Ok(session) => socket::run(Arc::clone(&inner), session).await,
            Err(error) => Err(error),
        };
        // Remove generation-owned identity and waiters before exposing the
        // disconnect notification. Event handlers must never observe an old
        // control ID after the socket that owned it has ended.
        clear_socket_state(&inner).await;
        if let Err(error) = &result {
            let _ = inner.event_tx.send(ClientEvent::Disconnected {
                reason: error.to_string(),
            });
        }
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

/// Completes exactly one request waiter without depending on response order.
fn resolve_pending(
    inner: &Arc<ConnectionInner>,
    request_id: String,
    result: Result<crate::CommandAck, RealtimeError>,
) {
    if let Ok(mut pending) = inner.pending.lock() {
        if let Some(sender) = pending.remove(&request_id) {
            let _ = sender.send(result);
        }
    }
}

/// Clears generation-owned identity and completes all waiters on socket loss.
async fn clear_socket_state(inner: &Arc<ConnectionInner>) {
    *inner.outbound.lock().await = None;
    if let Ok(mut control_id) = inner.control_id.write() {
        *control_id = None;
    }
    inner.access_expires_at.store(0, Ordering::Release);
    if let Ok(mut pending) = inner.pending.lock() {
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(RealtimeError::Closed));
        }
    }
}

/// Loads the next access lease from the injected provider.
async fn load_access_token(
    inner: &Arc<ConnectionInner>,
) -> Result<crate::AccessToken, RealtimeError> {
    let mut stop = inner.stop_tx.subscribe();
    if is_stopped(inner) {
        return Err(RealtimeError::Closed);
    }
    tokio::select! {
        result = inner.provider.access_token() => result,
        _changed = stop.changed() => Err(RealtimeError::Closed),
    }
}

/// Opens a transport while allowing host shutdown to cancel a slow connector.
async fn connect_socket(
    inner: &Arc<ConnectionInner>,
    access_token: crate::AccessToken,
) -> Result<Box<dyn crate::WebSocketSession>, RealtimeError> {
    let mut stop = inner.stop_tx.subscribe();
    if is_stopped(inner) {
        return Err(RealtimeError::Closed);
    }
    tokio::select! {
        result = inner.connector.connect(inner.websocket_url.clone(), access_token) => result,
        _changed = stop.changed() => Err(RealtimeError::Closed),
    }
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
    inner.status_tx.send_replace(status);
    let _ = inner.event_tx.send(ClientEvent::StatusChanged(status));
}
