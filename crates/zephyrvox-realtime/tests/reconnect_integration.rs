mod support;

use std::{collections::VecDeque, sync::Arc, time::Duration};

use support::{FakeConnector, FakeProvider, FakeSession};
use tokio::sync::{Mutex, mpsc};
use zephyrvox_realtime::{
    ClientEvent, ConnectionStatus, ControlConnection, RealtimeConfig, ReconnectPolicy,
    WebSocketMessage,
};
use zephyrvox_wire::{ServerCard, TransportScheme};

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
    let mut events = connection.subscribe_events();
    first_incoming_tx
        .send(WebSocketMessage::Close(Some("network reset".to_owned())))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                events.recv().await.unwrap(),
                ClientEvent::Disconnected { .. }
            ) {
                assert!(connection.control_connection_id().await.is_none());
                break;
            }
        }
    })
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

#[tokio::test]
async fn server_auth_expiry_close_stops_reconnect() {
    let card = ServerCard::new("localhost", 18080, TransportScheme::Plain, None, None).unwrap();
    let (first_incoming_tx, first_incoming_rx) = mpsc::channel(8);
    let (first_outgoing_tx, mut first_outgoing_rx) = mpsc::channel(8);
    first_incoming_tx
        .send(WebSocketMessage::Text(
            r#"{"type":"connection.ready","data":{"control_connection_id":"00000000000000000000000000000011","access_expires_at":4102444800000}}"#.to_owned(),
        ))
        .await
        .unwrap();
    first_incoming_tx
        .send(WebSocketMessage::CloseFrame {
            code: Some(4001),
            reason: Some("unauthorized".to_owned()),
        })
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
    let mut status = connection.subscribe_status();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if *status.borrow() == ConnectionStatus::Closed {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), second_outgoing_rx.recv())
            .await
            .is_err()
    );
}
