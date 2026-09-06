use crate::{ConnectionStatus, UnknownFrame};
use zephyrvox_types::{ControlConnectionId, StateEvent, VoiceAuthority};

/// Events emitted by a [`crate::ControlConnection`] in arrival order.
///
/// State events are also available through [`crate::StateStore::subscribe_events`]
/// when a host wants a domain-only stream. The combined stream is bounded and
/// may report a broadcast lag error to a slow subscriber; state convergence is
/// never blocked by that subscriber.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    /// A lifecycle status changed.
    StatusChanged(ConnectionStatus),
    /// The server assigned a fresh control connection identity.
    Ready {
        /// Server-assigned control connection ID.
        control_connection_id: ControlConnectionId,
        /// Access-lease expiry advertised by the server.
        access_expires_at: i64,
    },
    /// A complete snapshot/replay handoff must be performed.
    SyncRequired {
        /// Server or client diagnostic reason.
        reason: String,
    },
    /// One validated ordered state event was installed.
    State(StateEvent),
    /// The latest complete snapshot no longer contains the previously bound
    /// voice authority. Hosts should stop the old media session before acting
    /// on any replacement authority in the new state.
    VoiceLost {
        /// Authority that became invalid at the snapshot boundary.
        authority: VoiceAuthority,
    },
    /// A socket generation ended before another one was started.
    Disconnected {
        /// Diagnostic reason without credentials or voice keys.
        reason: String,
    },
    /// An unknown non-state frame was retained for host diagnostics.
    Unknown(UnknownFrame),
}
