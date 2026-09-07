use std::{sync::Arc, time::Duration};

use zephyrvox_sdk::{
    ApiClient, Client, ClientConfig, ClientEvent, ConnectionStatus, LoginRequest, Presence,
    ReconnectPolicy, SdkError, WebSocketMessage,
};

mod support;

use support::{FakeConnector, QueueHttp, card, envelope, login, snapshot, websocket_pair};

#[tokio::test(flavor = "current_thread")]
async fn facade_discovers_and_closes_lifecycle() {
    let transport = QueueHttp::new([envelope(support::metadata(19_001)), envelope(login())]);
    let card = card();
    let api = ApiClient::with_memory_transport(card.clone(), Arc::new(transport)).unwrap();
    let client = Client::with_api_client(ClientConfig::new(card), api).unwrap();

    let discovered = client.discover().await.unwrap();
    assert_eq!(discovered.codecs[0].sample_rate(), 48_000);
    client
        .login(LoginRequest {
            username: "owner".to_owned(),
            password: "password".to_owned(),
            device_id: "test".to_owned(),
        })
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    assert!(matches!(client.discover().await, Err(SdkError::Closed)));
}

#[tokio::test(flavor = "current_thread")]
async fn facade_connects_state_and_presence() {
    let transport = QueueHttp::new([envelope(login()), envelope(snapshot())]);
    let card = card();
    let api = ApiClient::with_memory_transport(card.clone(), Arc::new(transport)).unwrap();
    let client = Client::with_api_client(
        ClientConfig::new(card).with_reconnect_policy(ReconnectPolicy::disabled()),
        api,
    )
    .unwrap();
    client
        .login(LoginRequest {
            username: "owner".to_owned(),
            password: "password".to_owned(),
            device_id: "test".to_owned(),
        })
        .await
        .unwrap();

    let (session, server_tx, mut server_rx) = websocket_pair();
    let mut events = client.events();
    let connector = FakeConnector::default();
    connector.push(session).await;
    let control = client
        .connect_with_connector(Arc::new(connector))
        .await
        .unwrap();
    server_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "connection.ready",
                "data": {
                    "control_connection_id": "00000000000000000000000000000011",
                    "access_expires_at": 4102444800000_i64
                }
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let _hello = tokio::time::timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    server_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "sync.complete",
                "data": {"cursor": "cursor-0"}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    control.wait_until_live().await.unwrap();
    let mut saw_live = false;
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(1), events.recv()).await {
            Ok(Ok(ClientEvent::Connection(ConnectionStatus::Live))) => {
                saw_live = true;
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(saw_live);
    let state = client.state().unwrap();
    assert_eq!(state.borrow().server.name, "Test");

    let presence_client = client.clone();
    let presence_call = tokio::spawn(async move {
        presence_client
            .set_presence(Presence {
                status: "dnd".to_owned(),
                activity: None,
            })
            .await
    });
    let command = tokio::time::timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let command: serde_json::Value = serde_json::from_str(&command).unwrap();
    let request_id = command["request_id"].as_str().unwrap();
    server_tx
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "command.ok",
                "request_id": request_id,
                "data": {"command_type": "presence.set"}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    presence_call.await.unwrap().unwrap();
    assert_eq!(control.status(), ConnectionStatus::Live);
    client.shutdown().await.unwrap();
}
