mod support;

use std::sync::Arc;
use std::time::Duration;

use support::FakeTransport;
use zephyrvox_http::{AccessToken, ApiClient, HttpError, RefreshToken, TokenPair};
use zephyrvox_wire::{ServerCard, TransportScheme};

fn plain_card() -> ServerCard {
    ServerCard::new("example.test", 80, TransportScheme::Plain, None, None).expect("card")
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_preserves_state_headers_and_uses_bearer_access() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.header("authorization"), Some("Bearer access"));
        Ok(zephyrvox_http::HttpResponse::new(
            200,
            [
                ("etag".to_owned(), "\"state-7\"".to_owned()),
                ("x-zephyr-parent-etag".to_owned(), "\"parent-3\"".to_owned()),
                ("x-zephyr-command-id".to_owned(), "9".to_owned()),
                ("x-zephyr-state-cursor".to_owned(), "cursor-7".to_owned()),
                (
                    "x-zephyr-stream-epoch".to_owned(),
                    "00112233445566778899aabbccddeeff".to_owned(),
                ),
                ("x-zephyr-geid".to_owned(), "42".to_owned()),
                ("x-zephyr-sync-required".to_owned(), "false".to_owned()),
            ],
            serde_json::to_vec(&serde_json::json!({
                "code": 0,
                "message": "",
                "data": serde_json::from_str::<serde_json::Value>(
                    include_str!("fixtures/state-snapshot.json")
                )
                .expect("snapshot")
            }))
            .expect("JSON"),
        ))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client
        .set_tokens(
            TokenPair::new(
                AccessToken::new("access").expect("access"),
                RefreshToken::new("refresh").expect("refresh"),
                Duration::from_secs(300),
            )
            .expect("tokens"),
        )
        .await
        .expect("tokens");
    let snapshot = client.snapshot_response().await.expect("snapshot");
    assert_eq!(snapshot.data.geid.get(), 42);
    assert_eq!(
        snapshot.headers.etag.as_ref().expect("etag").as_str(),
        "\"state-7\""
    );
    assert_eq!(
        snapshot
            .headers
            .parent_etag
            .as_ref()
            .expect("parent etag")
            .as_str(),
        "\"parent-3\""
    );
    assert_eq!(snapshot.headers.command_id.as_deref(), Some("9"));
    assert!(snapshot.headers.state_cursor.is_some());
    assert!(snapshot.headers.stream_epoch.is_some());
    assert_eq!(snapshot.headers.geid.expect("geid").get(), 42);
    assert!(!snapshot.headers.sync_required);
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_state_header_is_reported_without_guessing() {
    let transport = FakeTransport::new(|_| {
        Ok(zephyrvox_http::HttpResponse::new(
            200,
            [("x-zephyr-geid".to_owned(), "not-a-number".to_owned())],
            serde_json::to_vec(&serde_json::json!({
                "code": 0,
                "message": "",
                "data": serde_json::from_str::<serde_json::Value>(
                    include_str!("fixtures/state-snapshot.json")
                )
                .expect("snapshot")
            }))
            .expect("JSON"),
        ))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client
        .set_tokens(
            TokenPair::new(
                AccessToken::new("access").expect("access"),
                RefreshToken::new("refresh").expect("refresh"),
                Duration::from_secs(300),
            )
            .expect("tokens"),
        )
        .await
        .expect("tokens");
    assert!(matches!(
        client.snapshot().await,
        Err(HttpError::InvalidHeader(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn request_parent_precondition_is_not_mistaken_for_response_etag() {
    let transport = FakeTransport::new(|_| {
        Ok(zephyrvox_http::HttpResponse::new(
            200,
            [(
                "x-zephyr-parent-if-match".to_owned(),
                "\"request-only\"".to_owned(),
            )],
            serde_json::to_vec(&serde_json::json!({
                "code": 0,
                "message": "",
                "data": serde_json::from_str::<serde_json::Value>(
                    include_str!("fixtures/state-snapshot.json")
                )
                .expect("snapshot")
            }))
            .expect("JSON"),
        ))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client
        .set_tokens(
            TokenPair::new(
                AccessToken::new("access").expect("access"),
                RefreshToken::new("refresh").expect("refresh"),
                Duration::from_secs(300),
            )
            .expect("tokens"),
        )
        .await
        .expect("tokens");

    let snapshot = client.snapshot_response().await.expect("snapshot");
    assert!(snapshot.headers.parent_etag.is_none());
}
