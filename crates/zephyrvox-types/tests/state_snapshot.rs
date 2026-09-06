use serde_json::json;
use zephyrvox_types::{StateEvent, StateSnapshot, StreamEpoch};

#[test]
fn snapshot_fixture_decodes_with_string_ids() {
    let snapshot: StateSnapshot =
        serde_json::from_str(include_str!("fixtures/state_snapshot.json")).expect("snapshot");
    assert_eq!(snapshot.geid.get(), 42);
    assert_eq!(snapshot.state.channels.len(), 1);
    assert_eq!(snapshot.state.self_state.user.id.get(), 123);
    assert_eq!(
        snapshot.stream_epoch,
        StreamEpoch::parse_hex("00112233445566778899aabbccddeeff").expect("epoch")
    );
    assert_eq!(
        snapshot
            .state
            .self_state
            .voice_authority
            .as_ref()
            .unwrap()
            .channel_id
            .get(),
        456
    );
}

#[test]
fn state_events_require_the_recipient_cursor() {
    let event = json!({
        "type": "state.event",
        "geid": "43",
        "class": "state",
        "scope": {"type": "server"},
        "event_type": "presence.updated",
        "server_time": 1,
        "data": {}
    });
    assert!(serde_json::from_value::<StateEvent>(event).is_err());
}
