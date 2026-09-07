mod support;

use std::{collections::VecDeque, sync::Arc, time::Duration};

use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use support::{FakeConnector, FakeProvider, FakeSession};
use zephyrvox_realtime::{
    AccessToken, ClientEvent, ConnectionStatus, ControlConnection, RealtimeConfig, RealtimeError,
    ReconnectPolicy, WebSocketMessage,
};
use zephyrvox_wire::{ServerCard, TransportScheme};

#[tokio::test]
async fn connection_performs_snapshot_handoff_and_correlates_commands() {
    let card = ServerCard::new("localhost", 18080, TransportScheme::Plain, None, None).unwrap();

    let (incoming_tx, incoming_rx) = mpsc::channel(16);
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(16);
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"connection.ready","data":{"control_connection_id":"00000000000000000000000000000011","access_expires_at":4102444800000}}"#.to_owned(),
        ))
        .await
        .unwrap();
    let connector = Arc::new(FakeConnector {
        sessions: Arc::new(Mutex::new(VecDeque::from([FakeSession {
            incoming: incoming_rx,
            outgoing: outgoing_tx,
        }]))),
    });
    let config = RealtimeConfig::new()
        .with_maximum_pending_commands(1)
        .unwrap()
        .with_reconnect_policy(ReconnectPolicy::disabled());
    let connection =
        ControlConnection::connect_with_connector(Arc::new(FakeProvider), card, config, connector)
            .await
            .unwrap();
    let mut events = connection.subscribe_events();
    let ready = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let ClientEvent::Ready {
                control_connection_id,
                access_expires_at,
            } = events.recv().await.unwrap()
            {
                break (control_connection_id, access_expires_at);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ready.0.to_hex(), "00000000000000000000000000000011");
    assert_eq!(ready.1, 4102444800000);

    let hello = outgoing_rx.recv().await.unwrap();
    let hello: Value = serde_json::from_str(&hello).unwrap();
    assert_eq!(hello["type"], "sync.hello");
    assert_eq!(hello["data"]["cursor"], "cursor-0");

    let renewal = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .update_access(AccessToken::new("access-during-sync").unwrap())
                .await
        }
    });
    let request = tokio::time::timeout(Duration::from_secs(1), outgoing_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let request: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(request["type"], "auth.update");
    assert_eq!(request["data"]["access_token"], "access-during-sync");
    let request_id = request["request_id"].as_str().unwrap();
    incoming_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "command.ok",
                "request_id": request_id,
                "data": {"command_type":"auth.update","expires_at":4102444800000_i64}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), renewal)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );

    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-0"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    connection.wait_until_live().await.unwrap();
    assert_eq!(connection.status(), ConnectionStatus::Live);

    let mut gap_status = connection.subscribe_status();
    incoming_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "state.event",
                "geid": "0",
                "cursor": "cursor-0",
                "class": "state",
                "scope": {"type": "server"},
                "event_type": "server.updated",
                "server_time": 1700000000002_i64,
                "data": {}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if *gap_status.borrow() == ConnectionStatus::Syncing {
                break;
            }
            gap_status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let hello = outgoing_rx.recv().await.unwrap();
    let hello: Value = serde_json::from_str(&hello).unwrap();
    assert_eq!(hello["type"], "sync.hello");
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-0"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    connection.wait_until_live().await.unwrap();

    let command = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .set_presence(zephyrvox_types::Presence {
                    status: "dnd".to_owned(),
                    activity: None,
                })
                .await
        }
    });
    let request = outgoing_rx.recv().await.unwrap();
    let request: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(request["type"], "presence.set");
    let request_id = request["request_id"].as_str().unwrap().to_owned();
    incoming_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "command.ok",
                "request_id": request_id,
                "data": {"command_type":"presence.set","command_id":"7"}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let ack = command.await.unwrap().unwrap();
    assert_eq!(ack.command_id.as_deref(), Some("7"));

    let update = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .update_access(AccessToken::new("access-2").unwrap())
                .await
        }
    });
    let request = outgoing_rx.recv().await.unwrap();
    let request: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(request["type"], "auth.update");
    assert_eq!(request["data"]["access_token"], "access-2");
    assert!(request["data"].get("refresh_token").is_none());
    let request_id = request["request_id"].as_str().unwrap();
    incoming_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "command.ok",
                "request_id": request_id,
                "data": {"command_type":"auth.update","expires_at":4102444800000_i64}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        update.await.unwrap().unwrap().expires_at,
        Some(4102444800000)
    );

    let mut statuses = connection.subscribe_status();
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.required","data":{"reason":"ring_miss"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    loop {
        let current = *statuses.borrow();
        if current == ConnectionStatus::Syncing {
            break;
        }
        statuses.changed().await.unwrap();
    }
    let hello = outgoing_rx.recv().await.unwrap();
    let hello: Value = serde_json::from_str(&hello).unwrap();
    assert_eq!(hello["type"], "sync.hello");
    assert_eq!(hello["data"]["cursor"], "cursor-0");
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-0"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    connection.wait_until_live().await.unwrap();

    let canceled = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .set_presence(zephyrvox_types::Presence {
                    status: "online".to_owned(),
                    activity: None,
                })
                .await
        }
    });
    let _ = tokio::time::timeout(Duration::from_secs(1), outgoing_rx.recv())
        .await
        .unwrap()
        .unwrap();
    canceled.abort();
    assert!(canceled.await.unwrap_err().is_cancelled());

    let admitted_after_cancel = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .set_presence(zephyrvox_types::Presence {
                    status: "dnd".to_owned(),
                    activity: None,
                })
                .await
        }
    });
    let _ = tokio::time::timeout(Duration::from_secs(1), outgoing_rx.recv())
        .await
        .unwrap()
        .unwrap();
    admitted_after_cancel.abort();
    assert!(admitted_after_cancel.await.unwrap_err().is_cancelled());

    let pending = tokio::spawn({
        let connection = connection.clone();
        async move {
            connection
                .set_presence(zephyrvox_types::Presence {
                    status: "afk".to_owned(),
                    activity: None,
                })
                .await
        }
    });
    let _ = outgoing_rx.recv().await.unwrap();
    connection.close().await.unwrap();
    assert!(matches!(pending.await.unwrap(), Err(RealtimeError::Closed)));
}

#[tokio::test]
async fn live_status_waits_until_events_queued_during_replay_are_applied() {
    let card = ServerCard::new("localhost", 18080, TransportScheme::Plain, None, None).unwrap();
    let (incoming_tx, incoming_rx) = mpsc::channel(8);
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"connection.ready","data":{"control_connection_id":"00000000000000000000000000000011","access_expires_at":4102444800000}}"#.to_owned(),
        ))
        .await
        .unwrap();
    let connector = Arc::new(FakeConnector {
        sessions: Arc::new(Mutex::new(VecDeque::from([FakeSession {
            incoming: incoming_rx,
            outgoing: outgoing_tx,
        }]))),
    });
    let config = RealtimeConfig::new().with_reconnect_policy(ReconnectPolicy::disabled());
    let connection =
        ControlConnection::connect_with_connector(Arc::new(FakeProvider), card, config, connector)
            .await
            .unwrap();
    let mut state_events = connection.state().subscribe_events();
    let _hello = outgoing_rx.recv().await.unwrap();

    incoming_tx
        .send(WebSocketMessage::Text(
            serde_json::to_string(&serde_json::json!({
                "type": "state.event",
                "geid": "1",
                "cursor": "cursor-1",
                "class": "state",
                "scope": {"type": "server"},
                "event_type": "server.updated",
                "server_time": 1700000000001_i64,
                "data": {}
            }))
            .unwrap(),
        ))
        .await
        .unwrap();
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-1"}}"#.to_owned(),
        ))
        .await
        .unwrap();

    connection.wait_until_live().await.unwrap();
    assert_eq!(connection.state().snapshot().await.geid.get(), 1);
    assert_eq!(state_events.recv().await.unwrap().geid.get(), 1);
    connection.close().await.unwrap();
}
