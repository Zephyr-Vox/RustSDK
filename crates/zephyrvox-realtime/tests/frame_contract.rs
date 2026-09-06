mod support;

use zephyrvox_realtime::{RealtimeError, ServerFrame, parse_server_frame};

#[test]
fn parses_ready_replay_and_command_frames() {
    let ready = parse_server_frame(
        r#"{"type":"connection.ready","data":{"control_connection_id":"00000000000000000000000000000011","access_expires_at":1700000000000}}"#,
    )
    .unwrap();
    assert!(matches!(ready, ServerFrame::ConnectionReady(_)));

    let replay = parse_server_frame(
        r#"{"type":"sync.replay","data":{"from_geid":"1","to_geid":"1","events":[{"type":"state.event","geid":"1","cursor":"cursor-1","class":"state","scope":{"type":"server"},"event_type":"server.updated","server_time":1,"data":{}}]}}"#,
    )
    .unwrap();
    match replay {
        ServerFrame::SyncReplay(replay) => {
            assert_eq!(replay.from_geid.get(), 1);
            assert_eq!(replay.events.len(), 1);
        }
        other => panic!("unexpected frame: {other:?}"),
    }

    let command = parse_server_frame(
        r#"{"type":"command.ok","request_id":"request-00000001","data":{"command_type":"presence.set","command_id":"7"}}"#,
    )
    .unwrap();
    match command {
        ServerFrame::CommandOk(ack) => {
            assert_eq!(ack.request_id, "request-00000001");
            assert_eq!(ack.command_id.as_deref(), Some("7"));
        }
        other => panic!("unexpected frame: {other:?}"),
    }

    let error = parse_server_frame(
        r#"{"type":"command.error","request_id":"request-00000002","data":{"command_type":"presence.set","code":1008,"message":"rate limited","retryable":true}}"#,
    )
    .unwrap();
    match error {
        ServerFrame::CommandError(error) => {
            assert_eq!(error.request_id, "request-00000002");
            assert_eq!(error.code, 1008);
            assert!(error.retryable);
        }
        other => panic!("unexpected frame: {other:?}"),
    }

    let live = parse_server_frame(
        r#"{"type":"state.event","geid":"1","cursor":"cursor-1","class":"state","scope":{"type":"server"},"event_type":"server.updated","server_time":1,"data":{}}"#,
    )
    .unwrap();
    assert!(matches!(live, ServerFrame::StateEvent(_)));
}

#[test]
fn rejects_request_ids_outside_the_server_grammar() {
    for request_id in [
        "too-short",
        "123456789012345!",
        "12345678901234567890123456789012345678901234567890123456789012345",
    ] {
        let payload = format!(
            r#"{{"type":"command.ok","request_id":"{request_id}","data":{{"command_type":"presence.set"}}}}"#
        );
        assert!(matches!(
            parse_server_frame(&payload),
            Err(RealtimeError::Protocol(message)) if message.contains("request_id")
        ));
    }
}

#[test]
fn unknown_non_state_frame_is_retained_but_unknown_state_is_not_silently_accepted() {
    let telemetry = parse_server_frame(r#"{"type":"voice.stats","data":{"packets":3}}"#).unwrap();
    assert!(matches!(telemetry, ServerFrame::Unknown(_)));

    let malformed = parse_server_frame(
        r#"{"type":"state.event","geid":"1","cursor":"cursor-1","class":"message","scope":{"type":"server"},"event_type":"server.updated","server_time":1,"data":{}}"#,
    );
    assert!(matches!(
        malformed,
        Ok(ServerFrame::StateEvent(event)) if event.class == "message"
    ));
}
