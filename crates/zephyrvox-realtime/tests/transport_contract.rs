use std::str::FromStr;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use zephyrvox_realtime::{
    AccessToken, RealtimeError, TokioTungsteniteConnector, WebSocketConnector,
};
use zephyrvox_wire::{ServerCard, TransportScheme};

#[tokio::test]
async fn production_connector_rejects_urls_outside_the_server_card() {
    let card = ServerCard::new("localhost", 18080, TransportScheme::Plain, None, None).unwrap();
    let connector = TokioTungsteniteConnector::new(card);
    let url = url::Url::from_str("ws://127.0.0.1:18080/api/v0/ws").unwrap();

    let result = connector
        .connect(url, AccessToken::new("access-token").unwrap())
        .await;

    assert!(matches!(
        result,
        Err(RealtimeError::Protocol(message))
            if message.contains("does not match server card authority")
    ));
}

#[tokio::test]
async fn production_connector_maps_unauthorized_upgrade_to_terminal_auth_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
    });

    let card = ServerCard::new("127.0.0.1", port, TransportScheme::Plain, None, None).unwrap();
    let connector = TokioTungsteniteConnector::new(card);
    let url = url::Url::parse(&format!("ws://127.0.0.1:{port}/api/v0/ws")).unwrap();
    let result = connector
        .connect(url, AccessToken::new("access-token").unwrap())
        .await;

    assert!(matches!(result, Err(RealtimeError::AuthenticationExpired)));
    server.await.unwrap();
}
