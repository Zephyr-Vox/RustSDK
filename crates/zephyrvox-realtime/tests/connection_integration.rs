mod support;

use std::{collections::VecDeque, sync::Arc, time::Duration};

use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use support::snapshot;
use zephyrvox_realtime::{
    AccessToken, BoxFuture, ClientEvent, ConnectionStatus, ControlConnection, RealtimeConfig,
    RealtimeError, ReconnectPolicy, SnapshotProvider, WebSocketConnector, WebSocketMessage,
    WebSocketSession,
};
use zephyrvox_types::StateSnapshot;
use zephyrvox_wire::{ServerCard, TransportScheme};

struct FakeProvider;

impl SnapshotProvider for FakeProvider {
    fn snapshot(&self) -> BoxFuture<'static, Result<StateSnapshot, RealtimeError>> {
        Box::pin(async { Ok(snapshot()) })
    }

    fn access_token(&self) -> BoxFuture<'static, Result<AccessToken, RealtimeError>> {
        Box::pin(async { AccessToken::new("access-1") })
    }
}

struct FakeSession {
    incoming: mpsc::Receiver<WebSocketMessage>,
    outgoing: mpsc::Sender<String>,
}

impl WebSocketSession for FakeSession {
    fn send_text<'a>(&'a mut self, text: String) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async move {
            self.outgoing
                .send(text)
                .await
                .map_err(|_| RealtimeError::Closed)
        })
    }

    fn send_pong<'a>(&'a mut self, _payload: Vec<u8>) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<WebSocketMessage>, RealtimeError>> {
        Box::pin(async move { Ok(self.incoming.recv().await) })
    }

    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async { Ok(()) })
    }
}

struct FakeConnector {
    sessions: Arc<Mutex<VecDeque<FakeSession>>>,
}

impl WebSocketConnector for FakeConnector {
    fn connect(
        &self,
        _url: url::Url,
        _access_token: AccessToken,
    ) -> BoxFuture<'static, Result<Box<dyn WebSocketSession>, RealtimeError>> {
        let session = Arc::clone(&self.sessions);
        Box::pin(async move {
            let mut sessions = session.lock().await;
            let session = sessions.pop_front().ok_or(RealtimeError::Closed)?;
            Ok(Box::new(session) as Box<dyn WebSocketSession>)
        })
    }
}

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
    let config = RealtimeConfig::new().with_reconnect_policy(ReconnectPolicy::disabled());
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
    incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-0"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    connection.wait_until_live().await.unwrap();
    assert_eq!(connection.status(), ConnectionStatus::Live);

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
async fn reconnect_obtains_a_new_control_id_and_replays_from_the_latest_snapshot() {
    let card = ServerCard::new("localhost", 18080, TransportScheme::Plain, None, None).unwrap();

    let (first_incoming_tx, first_incoming_rx) = mpsc::channel(8);
    let (first_outgoing_tx, mut first_outgoing_rx) = mpsc::channel(8);
    first_incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"connection.ready","data":{"control_connection_id":"00000000000000000000000000000011","access_expires_at":4102444800000}}"#.to_owned(),
        ))
        .await
        .unwrap();
    let (second_incoming_tx, second_incoming_rx) = mpsc::channel(8);
    let (second_outgoing_tx, mut second_outgoing_rx) = mpsc::channel(8);
    second_incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"connection.ready","data":{"control_connection_id":"00000000000000000000000000000022","access_expires_at":4102444800000}}"#.to_owned(),
        ))
        .await
        .unwrap();
    let connector = Arc::new(FakeConnector {
        sessions: Arc::new(Mutex::new(VecDeque::from([
            FakeSession {
                incoming: first_incoming_rx,
                outgoing: first_outgoing_tx,
            },
            FakeSession {
                incoming: second_incoming_rx,
                outgoing: second_outgoing_tx,
            },
        ]))),
    });
    let policy = ReconnectPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(1),
        Duration::ZERO,
    )
    .unwrap()
    .with_maximum_attempts(Some(1));
    let config = RealtimeConfig::new().with_reconnect_policy(policy);
    let connection =
        ControlConnection::connect_with_connector(Arc::new(FakeProvider), card, config, connector)
            .await
            .unwrap();

    let _ = first_outgoing_rx.recv().await.unwrap();
    first_incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-0"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    connection.wait_until_live().await.unwrap();
    assert_eq!(
        connection.control_connection_id().await.unwrap().to_hex(),
        "00000000000000000000000000000011"
    );

    let mut statuses = connection.subscribe_status();
    first_incoming_tx
        .send(WebSocketMessage::Close(Some("network reset".to_owned())))
        .await
        .unwrap();
    loop {
        let current = *statuses.borrow();
        if current == ConnectionStatus::Reconnecting {
            break;
        }
        statuses.changed().await.unwrap();
    }
    let _ = second_outgoing_rx.recv().await.unwrap();
    second_incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"sync.complete","data":{"cursor":"cursor-0"}}"#.to_owned(),
        ))
        .await
        .unwrap();
    connection.wait_until_live().await.unwrap();
    assert_eq!(
        connection.control_connection_id().await.unwrap().to_hex(),
        "00000000000000000000000000000022"
    );

    connection.close().await.unwrap();
}
