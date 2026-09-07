use std::{sync::Arc, time::Duration};

use zephyrvox_sdk::{
    ApiClient, Client, ClientConfig, ClientEvent, ConnectionStatus, PacketCodec, ReconnectPolicy,
    SessionKey, StreamTypeId, VoiceEndpoint, VoiceEvent, VoiceJoinRequest, VoiceSessionConfig,
    VoiceSessionId, VoiceStatus, WebSocketMessage,
};

mod support;

use support::{
    FakeConnector, QueueHttp, card, envelope, login, metadata, snapshot, voice_join, websocket_pair,
};

#[tokio::test(flavor = "current_thread")]
async fn facade_binds_voice_to_metadata_and_receives_media() {
    let voice_socket = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let voice_port = voice_socket.local_addr().unwrap().port();
    let mut reused_join = voice_join("10", true);
    reused_join["voice"]["created"] = serde_json::Value::Bool(false);
    reused_join["voice"]["key"] = serde_json::Value::Null;
    let transport = QueueHttp::new([
        envelope(metadata(voice_port)),
        envelope(login()),
        envelope(snapshot()),
        envelope(voice_join("10", true)),
        envelope(reused_join.clone()),
        envelope(reused_join),
        zephyrvox_sdk::HttpResponse::empty(204),
    ]);
    let card = card();
    let api = ApiClient::with_memory_transport(card.clone(), Arc::new(transport)).unwrap();
    let client = Client::with_api_client(ClientConfig::new(card), api).unwrap();
    client.discover().await.unwrap();
    client
        .login(zephyrvox_sdk::LoginRequest {
            username: "owner".to_owned(),
            password: "password".to_owned(),
            device_id: "test".to_owned(),
        })
        .await
        .unwrap();
    let (session, server_tx, mut server_rx) = websocket_pair();
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

    let voice = client
        .join_voice(
            zephyrvox_sdk::Snowflake::new(10).unwrap(),
            VoiceJoinRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(voice.codec().sample_rate(), 48_000);
    assert_eq!(voice.status(), VoiceStatus::Active);

    let mut heartbeat = [0_u8; 1_200];
    let (_, client_addr) = tokio::time::timeout(
        Duration::from_secs(1),
        voice_socket.recv_from(&mut heartbeat),
    )
    .await
    .unwrap()
    .unwrap();
    let server_config = VoiceSessionConfig::new_with_protocol_version(
        VoiceEndpoint {
            host: "127.0.0.1".to_owned(),
            port: voice_port,
        },
        VoiceSessionId::parse_hex("00000000000000000000000000000022").unwrap(),
        1,
        true,
        100,
        4102444800000,
        Some(SessionKey::from_bytes([7_u8; 32])),
    )
    .unwrap()
    .with_stream_types([StreamTypeId::new(1)])
    .unwrap();
    let server_codec = PacketCodec::from_config(&server_config, None).unwrap();
    let packet = server_codec
        .encode_server_media(
            1,
            1,
            StreamTypeId::new(1),
            zephyrvox_sdk::Snowflake::new(2).unwrap(),
            b"encoded",
        )
        .unwrap();
    voice_socket.send_to(&packet, client_addr).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), voice.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.payload.as_ref(), b"encoded");
    assert_eq!(
        received.speaker_id,
        zephyrvox_sdk::Snowflake::new(2).unwrap()
    );
    let replaced = client
        .join_voice(
            zephyrvox_sdk::Snowflake::new(10).unwrap(),
            VoiceJoinRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(replaced.session_id(), voice.session_id());
    assert!(voice.status().is_terminal());
    let replaced_again = client
        .join_voice(
            zephyrvox_sdk::Snowflake::new(10).unwrap(),
            VoiceJoinRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(replaced_again.session_id(), voice.session_id());
    assert_eq!(
        client.voice_session().await.unwrap().session_id(),
        voice.session_id()
    );
    client.leave_voice().await.unwrap();
    assert!(voice.status().is_terminal());
    client.shutdown().await.unwrap();
    drop(voice_socket);
}

#[tokio::test(flavor = "current_thread")]
async fn facade_rejoins_voice_only_after_an_ordinary_reconnect() {
    let voice_socket = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let voice_port = voice_socket.local_addr().unwrap().port();
    let transport = QueueHttp::new([
        envelope(metadata(voice_port)),
        envelope(login()),
        envelope(snapshot()),
        envelope(voice_join("10", true)),
        envelope(voice_join("10", true)),
    ]);
    let card = card();
    let api = ApiClient::with_memory_transport(card.clone(), Arc::new(transport)).unwrap();
    let client = Client::with_api_client(
        ClientConfig::new(card)
            .with_auto_rejoin_voice(true)
            .with_reconnect_policy(
                ReconnectPolicy::new(
                    Duration::from_millis(1),
                    Duration::from_millis(1),
                    Duration::ZERO,
                )
                .unwrap()
                .with_maximum_attempts(Some(1)),
            ),
        api,
    )
    .unwrap();
    client.discover().await.unwrap();
    client
        .login(zephyrvox_sdk::LoginRequest {
            username: "owner".to_owned(),
            password: "password".to_owned(),
            device_id: "test".to_owned(),
        })
        .await
        .unwrap();

    let (first_session, first_server_tx, mut first_server_rx) = websocket_pair();
    let (second_session, second_server_tx, mut second_server_rx) = websocket_pair();
    let connector = FakeConnector::default();
    connector.push(first_session).await;
    connector.push(second_session).await;
    let control = client
        .connect_with_connector(Arc::new(connector))
        .await
        .unwrap();
    send_ready(&first_server_tx, "11").await;
    let _ = tokio::time::timeout(Duration::from_secs(1), first_server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    send_sync_complete(&first_server_tx).await;
    control.wait_until_live().await.unwrap();
    client
        .join_voice(
            zephyrvox_sdk::Snowflake::new(10).unwrap(),
            VoiceJoinRequest::default(),
        )
        .await
        .unwrap();

    let mut events = client.events();
    send_ready(&second_server_tx, "22").await;
    first_server_tx
        .send(WebSocketMessage::Close(Some("network reset".to_owned())))
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), second_server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    send_sync_complete(&second_server_tx).await;

    let mut saw_reconnect = false;
    let mut saw_rejoin = false;
    for _ in 0..32 {
        match tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            ClientEvent::Connection(ConnectionStatus::Reconnecting) => saw_reconnect = true,
            ClientEvent::Voice(VoiceEvent::Joined { .. }) => {
                saw_rejoin = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_reconnect);
    assert!(saw_rejoin);
    assert!(client.voice_session().await.is_some());
    client.shutdown().await.unwrap();
    drop(voice_socket);
}

async fn send_ready(sender: &tokio::sync::mpsc::Sender<WebSocketMessage>, id_suffix: &str) {
    sender
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "connection.ready",
                "data": {
                    "control_connection_id": format!("000000000000000000000000000000{id_suffix}"),
                    "access_expires_at": 4102444800000_i64
                }
            })
            .to_string(),
        ))
        .await
        .unwrap();
}

async fn send_sync_complete(sender: &tokio::sync::mpsc::Sender<WebSocketMessage>) {
    sender
        .send(WebSocketMessage::Text(
            serde_json::json!({
                "type": "sync.complete",
                "data": {"cursor": "cursor-0"}
            })
            .to_string(),
        ))
        .await
        .unwrap();
}
