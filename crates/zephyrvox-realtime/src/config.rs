use std::time::Duration;

/// The lifecycle phase currently observable by an application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// No transport attempt is active.
    Disconnected,
    /// A WebSocket transport is being opened.
    Connecting,
    /// The server accepted the socket and sent `connection.ready`.
    Ready,
    /// The socket is replaying state or waiting for a replacement snapshot.
    Syncing,
    /// The state projection is synchronized and live events are accepted.
    Live,
    /// The previous socket ended and the reconnect backoff is running.
    Reconnecting,
    /// The connection will not make another attempt.
    Closed,
}

/// Exponential reconnect policy with bounded delay and optional jitter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    enabled: bool,
    minimum_delay: Duration,
    maximum_delay: Duration,
    jitter: Duration,
    maximum_attempts: Option<usize>,
}

impl ReconnectPolicy {
    /// Creates an enabled policy.
    ///
    /// `minimum_delay` must be non-zero and no greater than
    /// `maximum_delay`. `jitter` may be zero for deterministic tests.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the delay range is invalid.
    pub fn new(
        minimum_delay: Duration,
        maximum_delay: Duration,
        jitter: Duration,
    ) -> Result<Self, crate::RealtimeError> {
        if minimum_delay.is_zero() || maximum_delay < minimum_delay {
            return Err(crate::RealtimeError::Configuration(
                "reconnect delays must be non-zero and ordered".to_owned(),
            ));
        }
        Ok(Self {
            enabled: true,
            minimum_delay,
            maximum_delay,
            jitter,
            maximum_attempts: None,
        })
    }

    /// Creates a disabled policy that never starts a second socket attempt.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            minimum_delay: Duration::from_secs(1),
            maximum_delay: Duration::from_secs(1),
            jitter: Duration::ZERO,
            maximum_attempts: Some(0),
        }
    }

    /// Returns the default policy used by [`RealtimeConfig`].
    pub const fn default_policy() -> Self {
        Self {
            enabled: true,
            minimum_delay: Duration::from_millis(250),
            maximum_delay: Duration::from_secs(30),
            jitter: Duration::from_millis(250),
            maximum_attempts: None,
        }
    }

    /// Limits the number of reconnect attempts while preserving other values.
    /// `None` means unlimited attempts.
    pub const fn with_maximum_attempts(mut self, attempts: Option<usize>) -> Self {
        self.maximum_attempts = attempts;
        self
    }

    /// Returns whether automatic reconnect is enabled.
    pub const fn enabled(self) -> bool {
        self.enabled
    }

    /// Returns the configured maximum number of attempts, if bounded.
    pub const fn maximum_attempts(self) -> Option<usize> {
        self.maximum_attempts
    }

    /// Computes the delay before one zero-based reconnect attempt.
    ///
    /// The exponential component is deterministic. Jitter is derived from
    /// the current monotonic clock only to avoid another runtime dependency;
    /// callers that need deterministic behavior set jitter to zero.
    pub fn delay(self, attempt: usize) -> Duration {
        let exponent = attempt.min(31) as u32;
        let multiplier = 1_u128 << exponent;
        let base_nanos = self.minimum_delay.as_nanos().saturating_mul(multiplier);
        let bounded = base_nanos.min(self.maximum_delay.as_nanos());
        let jitter = if self.jitter.is_zero() {
            0
        } else {
            let sample = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos();
            sample % self.jitter.as_nanos().max(1)
        };
        Duration::from_nanos(
            bounded
                .saturating_add(jitter)
                .min(self.maximum_delay.as_nanos())
                .min(u64::MAX as u128) as u64,
        )
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

/// Limits and lifecycle behavior for one [`crate::ControlConnection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeConfig {
    reconnect: ReconnectPolicy,
    command_timeout: Duration,
    maximum_pending_commands: usize,
    maximum_frame_bytes: usize,
}

impl RealtimeConfig {
    /// Creates the standard v1 configuration.
    pub const fn new() -> Self {
        Self {
            reconnect: ReconnectPolicy::default_policy(),
            command_timeout: Duration::from_secs(10),
            maximum_pending_commands: 64,
            maximum_frame_bytes: 256 * 1024,
        }
    }

    /// Replaces the reconnect policy.
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// Replaces the command response timeout.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for a zero timeout.
    pub fn with_command_timeout(mut self, timeout: Duration) -> Result<Self, crate::RealtimeError> {
        if timeout.is_zero() {
            return Err(crate::RealtimeError::Configuration(
                "command timeout must be non-zero".to_owned(),
            ));
        }
        self.command_timeout = timeout;
        Ok(self)
    }

    /// Replaces the bounded number of command requests that may wait for ACKs.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for zero or excessively large limits.
    pub fn with_maximum_pending_commands(
        mut self,
        maximum: usize,
    ) -> Result<Self, crate::RealtimeError> {
        if maximum == 0 || maximum > 256 {
            return Err(crate::RealtimeError::Configuration(
                "pending command limit must be between 1 and 256".to_owned(),
            ));
        }
        self.maximum_pending_commands = maximum;
        Ok(self)
    }

    /// Replaces the maximum accepted inbound text frame size.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the limit is zero or exceeds the
    /// server's v1 hard limit.
    pub fn with_maximum_frame_bytes(
        mut self,
        maximum: usize,
    ) -> Result<Self, crate::RealtimeError> {
        if maximum == 0 || maximum > 256 * 1024 {
            return Err(crate::RealtimeError::Configuration(
                "frame limit must be between 1 byte and 256 KiB".to_owned(),
            ));
        }
        self.maximum_frame_bytes = maximum;
        Ok(self)
    }

    /// Returns the reconnect policy.
    pub const fn reconnect_policy(self) -> ReconnectPolicy {
        self.reconnect
    }

    /// Returns the command response timeout.
    pub const fn command_timeout(self) -> Duration {
        self.command_timeout
    }

    /// Returns the pending command bound.
    pub const fn maximum_pending_commands(self) -> usize {
        self.maximum_pending_commands
    }

    /// Returns the inbound text-frame bound.
    pub const fn maximum_frame_bytes(self) -> usize {
        self.maximum_frame_bytes
    }
}

impl Default for RealtimeConfig {
    fn default() -> Self {
        Self::new()
    }
}
