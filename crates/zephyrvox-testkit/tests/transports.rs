use std::time::Duration;

use tokio::time::timeout;

use zephyrvox_http::{HttpMethod, HttpRequest, HttpTransport};
use zephyrvox_realtime::{WebSocketMessage, WebSocketSession};
use zephyrvox_testkit::{FakeHttpTransport, UdpPair};

#[tokio::test(flavor = "current_thread")]
async fn fake_http_transport_consumes_responses_in_order() {
    let transport = FakeHttpTransport::new();
    transport
        .push_response(FakeHttpTransport::envelope(serde_json::json!({"ok": true})))
        .await;
    let request = HttpRequest::new(
        HttpMethod::Get,
        url::Url::parse("http://localhost/api/v0/metadata").unwrap(),
    );
    let response = transport.send(request).await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.body().windows(4).any(|window| window == b"code"));
}

#[tokio::test(flavor = "current_thread")]
async fn fake_websocket_pair_is_bidirectional() {
    let (mut session, peer) = zephyrvox_testkit::websocket_pair(2);
    session.send_text("client-frame".to_owned()).await.unwrap();
    let mut peer = peer;
    assert_eq!(peer.recv().await.as_deref(), Some("client-frame"));
    peer.send(WebSocketMessage::Ping(vec![1, 2])).await.unwrap();
    assert_eq!(
        session.receive().await.unwrap(),
        Some(WebSocketMessage::Ping(vec![1, 2]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn udp_pair_connects_both_sides() {
    let (client, server) = UdpPair::bind().await.unwrap().into_parts();
    client.send(b"voice").await.unwrap();
    let mut payload = [0_u8; 16];
    let size = timeout(Duration::from_secs(1), server.recv(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload[..size], b"voice");
}
