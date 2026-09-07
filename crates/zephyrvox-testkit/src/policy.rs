//! Deterministic lifecycle policies used by transport tests.

use std::time::Duration;

use zephyrvox_realtime::ReconnectPolicy;

/// Creates a reconnect policy with no clock jitter and a one-millisecond
/// delay, making retry tests deterministic and fast.
pub fn deterministic_reconnect_policy(maximum_attempts: Option<usize>) -> ReconnectPolicy {
    ReconnectPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(1),
        Duration::ZERO,
    )
    .expect("test reconnect policy is valid")
    .with_maximum_attempts(maximum_attempts)
}
