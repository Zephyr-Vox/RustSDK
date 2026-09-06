use std::time::Duration;

use zephyrvox_realtime::{RealtimeConfig, ReconnectPolicy};

#[test]
fn reconnect_policy_is_bounded_and_config_limits_are_validated() {
    let policy = ReconnectPolicy::new(
        Duration::from_millis(10),
        Duration::from_millis(40),
        Duration::ZERO,
    )
    .unwrap();
    assert_eq!(policy.delay(0), Duration::from_millis(10));
    assert_eq!(policy.delay(1), Duration::from_millis(20));
    assert_eq!(policy.delay(2), Duration::from_millis(40));
    assert_eq!(policy.delay(10), Duration::from_millis(40));

    assert!(ReconnectPolicy::new(Duration::ZERO, Duration::from_secs(1), Duration::ZERO).is_err());
    assert!(
        RealtimeConfig::new()
            .with_maximum_pending_commands(0)
            .is_err()
    );
    assert!(
        RealtimeConfig::new()
            .with_maximum_frame_bytes(256 * 1024 + 1)
            .is_err()
    );
}
