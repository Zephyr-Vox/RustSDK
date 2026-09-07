mod support;

use support::{control_id, empty_client_state, epoch, state_event};
use zephyrvox_realtime::{SyncComplete, SyncError, SyncMachine, SyncPhase, SyncReplay};
use zephyrvox_types::Cursor;

#[test]
fn replay_handoff_requires_order_and_releases_live_queue_after_complete() {
    let mut machine = SyncMachine::new(&empty_client_state());
    assert!(machine.check_stream_epoch(epoch()).is_ok());
    machine.begin_connecting().unwrap();
    machine.accept_ready(control_id()).unwrap();
    assert_eq!(machine.phase(), SyncPhase::Ready);
    assert_eq!(machine.begin_sync().unwrap().as_str(), "cursor-0");

    let replay_event = state_event(1, "server.updated", serde_json::json!({}));
    machine
        .accept_replay(&SyncReplay {
            from_geid: replay_event.geid,
            to_geid: replay_event.geid,
            events: vec![replay_event],
        })
        .unwrap();

    let live_event = state_event(2, "server.updated", serde_json::json!({}));
    assert!(machine.accept_live_event(live_event).unwrap().is_none());
    let released = machine
        .accept_complete(&SyncComplete {
            cursor: Cursor::new("cursor-2").unwrap(),
        })
        .unwrap();
    assert_eq!(machine.phase(), SyncPhase::Live);
    assert_eq!(released.len(), 1);
    assert_eq!(machine.geid().get(), 2);
}

#[test]
fn replay_rejects_non_monotonic_events_and_inconsistent_bounds() {
    let mut machine = SyncMachine::new(&empty_client_state());
    machine.begin_connecting().unwrap();
    machine.accept_ready(control_id()).unwrap();
    machine.begin_sync().unwrap();

    let first = state_event(1, "server.updated", serde_json::json!({}));
    let second = state_event(1, "server.updated", serde_json::json!({}));
    let result = machine.accept_replay(&SyncReplay {
        from_geid: first.geid,
        to_geid: second.geid,
        events: vec![first, second],
    });
    assert!(result.is_err());
}

#[test]
fn replay_accepts_geid_gaps_created_by_visibility_filtering() {
    let mut machine = SyncMachine::new(&empty_client_state());
    machine.begin_connecting().unwrap();
    machine.accept_ready(control_id()).unwrap();
    machine.begin_sync().unwrap();

    let event = state_event(2, "server.updated", serde_json::json!({}));
    machine
        .accept_replay(&SyncReplay {
            from_geid: event.geid,
            to_geid: event.geid,
            events: vec![event],
        })
        .unwrap();
}

#[test]
fn live_events_are_bounded_until_replay_completes() {
    let mut machine = SyncMachine::new(&empty_client_state());
    machine.begin_connecting().unwrap();
    machine.accept_ready(control_id()).unwrap();
    machine.begin_sync().unwrap();

    for geid in 1..=256 {
        assert!(
            machine
                .accept_live_event(state_event(geid, "server.updated", serde_json::json!({})))
                .unwrap()
                .is_none()
        );
    }

    assert!(matches!(
        machine.accept_live_event(state_event(257, "server.updated", serde_json::json!({}))),
        Err(SyncError::QueueLimit)
    ));
}
