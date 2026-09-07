//! Host-facing lifecycle and state event stream.

use std::sync::Weak;

use tokio::sync::broadcast;

use zephyrvox_realtime::{ClientEvent as RealtimeClientEvent, ConnectionStatus};
use zephyrvox_types::{ControlConnectionId, StateEvent, VoiceAuthority, VoiceSessionId};

use crate::{Client, client::ClientInner};

/// Events emitted by a [`crate::Client`] in control-plane order.
///
/// State events retain the realtime crate's validated payload.  UDP media is
/// intentionally delivered through [`crate::VoiceSession::recv`] rather than
/// this control stream so a slow audio consumer cannot delay state delivery.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    /// The control connection changed lifecycle phase.
    Connection(ConnectionStatus),
    /// The server assigned a new control-connection identity.
    Ready {
        /// Server-assigned control connection identifier.
        control_connection_id: ControlConnectionId,
        /// Access lease expiry in Unix milliseconds.
        access_expires_at: i64,
    },
    /// Synchronization requires a complete snapshot/replay handoff.
    SyncRequired {
        /// Diagnostic reason supplied by the synchronizer or server.
        reason: String,
    },
    /// One validated ordered state event was installed.
    State(StateEvent),
    /// A voice authority or local media session changed lifecycle.
    Voice(VoiceEvent),
    /// A socket generation ended before a replacement was ready.
    Disconnected {
        /// Sanitized diagnostic reason.
        reason: String,
    },
    /// An unknown forward-compatible frame was retained for diagnostics.
    Unknown(zephyrvox_realtime::UnknownFrame),
    /// A bounded facade event relay experienced lag or another non-terminal
    /// diagnostic condition.
    Error {
        /// Sanitized diagnostic message that contains no credentials or keys.
        message: String,
    },
}

/// Voice-specific events that share the facade event channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceEvent {
    /// A new media session became available to the host.
    Joined {
        /// Voice channel containing the session.
        channel_id: zephyrvox_types::Snowflake,
        /// Server-assigned voice session identifier.
        session_id: VoiceSessionId,
    },
    /// The previous state snapshot no longer authorizes a voice session.
    Lost {
        /// Authority invalidated at the snapshot boundary.
        authority: VoiceAuthority,
    },
}

impl From<RealtimeClientEvent> for ClientEvent {
    fn from(event: RealtimeClientEvent) -> Self {
        match event {
            RealtimeClientEvent::StatusChanged(status) => Self::Connection(status),
            RealtimeClientEvent::Ready {
                control_connection_id,
                access_expires_at,
            } => Self::Ready {
                control_connection_id,
                access_expires_at,
            },
            RealtimeClientEvent::SyncRequired { reason } => Self::SyncRequired { reason },
            RealtimeClientEvent::State(event) => Self::State(event),
            RealtimeClientEvent::VoiceLost { authority } => {
                Self::Voice(VoiceEvent::Lost { authority })
            }
            RealtimeClientEvent::Disconnected { reason } => Self::Disconnected { reason },
            RealtimeClientEvent::Unknown(frame) => Self::Unknown(frame),
        }
    }
}

/// A bounded receiver for high-level client events.
///
/// `EventStream` is deliberately a small wrapper around Tokio broadcast.  A
/// host may use `recv` in an async event loop or `try_recv` from a TUI tick;
/// when a receiver falls behind, Tokio's `Lagged` error remains observable.
pub struct EventStream {
    receiver: broadcast::Receiver<ClientEvent>,
}

impl EventStream {
    /// Receives the next event, preserving Tokio broadcast lag semantics.
    pub async fn recv(&mut self) -> Result<ClientEvent, broadcast::error::RecvError> {
        self.receiver.recv().await
    }

    /// Tries to receive an event without waiting.
    pub fn try_recv(&mut self) -> Result<ClientEvent, broadcast::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Creates an independent cursor at the current event boundary.
    pub fn resubscribe(&self) -> Self {
        Self {
            receiver: self.receiver.resubscribe(),
        }
    }

    /// Returns the number of events that can be retained by the facade relay.
    pub const fn capacity() -> usize {
        256
    }
}

impl From<broadcast::Receiver<ClientEvent>> for EventStream {
    fn from(receiver: broadcast::Receiver<ClientEvent>) -> Self {
        Self { receiver }
    }
}

/// Relays realtime events and tears down media when its owner loses authority.
pub(crate) async fn relay_realtime_events(
    weak: Weak<ClientInner>,
    mut receiver: broadcast::Receiver<RealtimeClientEvent>,
    sender: broadcast::Sender<ClientEvent>,
    generation: u64,
) {
    loop {
        let Some(inner) = weak.upgrade() else {
            break;
        };
        if inner
            .connection_generation
            .load(std::sync::atomic::Ordering::Acquire)
            != generation
        {
            break;
        }
        let event = match receiver.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                let _ = sender.send(ClientEvent::Error {
                    message: format!("realtime event relay lagged by {missed} events"),
                });
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        let stop_voice = matches!(
            &event,
            RealtimeClientEvent::VoiceLost { .. }
                | RealtimeClientEvent::Disconnected { .. }
                | RealtimeClientEvent::StatusChanged(
                    ConnectionStatus::Reconnecting | ConnectionStatus::Closed
                )
        );
        // Ordinary disconnects first enter Reconnecting and retain opt-in
        // intent. A terminal Closed status covers explicit close, auth
        // revocation, and exhausted retries, so it must discard that intent.
        let clear_voice_intent = matches!(
            &event,
            RealtimeClientEvent::VoiceLost { .. }
                | RealtimeClientEvent::StatusChanged(ConnectionStatus::Closed)
        );
        let rejoin_voice = matches!(
            &event,
            RealtimeClientEvent::StatusChanged(ConnectionStatus::Live)
        );
        let _ = sender.send(ClientEvent::from(event));
        let Some(inner) = weak.upgrade() else {
            break;
        };
        if inner
            .connection_generation
            .load(std::sync::atomic::Ordering::Acquire)
            != generation
        {
            continue;
        }
        let client = Client { inner };
        if clear_voice_intent {
            client.clear_voice_intent();
        }
        if stop_voice {
            if let Err(error) = client.stop_voice_transport_for(Some(generation)).await {
                let _ = sender.send(ClientEvent::Error {
                    message: error.to_string(),
                });
            }
        }
        if rejoin_voice {
            if let Err(error) = client.rejoin_voice_if_configured().await {
                let _ = sender.send(ClientEvent::Error {
                    message: error.to_string(),
                });
            }
        }
    }
}
