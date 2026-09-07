use crate::RevocationReason;

/// The locally observable lifecycle of one negotiated UDP voice session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceStatus {
    /// No UDP task has been started.
    Idle,
    /// The UDP task is sending its first heartbeat.
    Joining,
    /// The session is allowed to send and receive media.
    Active,
    /// The host requested an orderly stop.
    Disconnecting,
    /// The session stopped without an explicit revocation notice.
    Disconnected,
    /// The server or control plane explicitly revoked the session.
    Revoked(RevocationReason),
}

impl VoiceStatus {
    /// Reports whether this status cannot transition back to active.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Disconnected | Self::Revoked(_))
    }

    /// Reports whether media commands are currently admissible.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}
