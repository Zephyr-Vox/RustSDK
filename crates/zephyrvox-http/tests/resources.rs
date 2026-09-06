mod support;

use std::{sync::Arc, time::Duration};

use serde_json::Value;
use support::{FakeTransport, envelope};
use zephyrvox_http::{
    AccessPrincipal, AccessToken, ApiClient, CreateGroupRequest, HttpError, RequestOptions,
    TokenPair, UpdateChannelRequest, UpdateGroupRequest,
};
use zephyrvox_types::{Etag, Snowflake};
use zephyrvox_wire::{ServerCard, TransportScheme};

fn plain_card() -> ServerCard {
    ServerCard::new("example.test", 80, TransportScheme::Plain, None, None).expect("card")
}

fn id(value: u64) -> Snowflake {
    Snowflake::new(value).expect("snowflake")
}

fn tokens() -> TokenPair {
    TokenPair::new(
        AccessToken::new("access").expect("access"),
        zephyrvox_http::RefreshToken::new("refresh").expect("refresh"),
        Duration::from_secs(300),
    )
    .expect("tokens")
}

#[tokio::test(flavor = "current_thread")]
async fn group_create_and_channel_update_apply_wire_headers_and_ids() {
    let transport = FakeTransport::new(|request| match (request.method(), request.url().path()) {
        (zephyrvox_http::HttpMethod::Post, "/api/v0/groups") => {
            assert_eq!(request.header("authorization"), Some("Bearer access"));
            assert!(
                request
                    .header("idempotency-key")
                    .is_some_and(|value| value.len() >= 16)
            );
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("group body")).expect("JSON"),
                serde_json::json!({"name":"Lobby","position":1,"visibility":"public"})
            );
            Ok(envelope(serde_json::json!({
                "id": "101",
                "name": "Lobby",
                "position": 1,
                "visibility": "public",
                "version": "1"
            })))
        }
        (zephyrvox_http::HttpMethod::Patch, "/api/v0/channels/102") => {
            assert_eq!(request.header("if-match"), Some("\"channel-1\""));
            assert_eq!(
                request.header("x-zephyr-parent-if-match"),
                Some("\"group-1\"")
            );
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("channel body"))
                    .expect("JSON"),
                serde_json::json!({"group_id":null,"pinned":true})
            );
            Ok(envelope(serde_json::json!({
                "id": "102",
                "group_id": null,
                "name": "Voice",
                "mode": "voice",
                "temporary": false,
                "visibility": "public",
                "capacity": 32,
                "position": 1,
                "pinned": true,
                "version": "2"
            })))
        }
        path => panic!("unexpected request {path:?}"),
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    let group = client
        .groups()
        .create(
            CreateGroupRequest {
                name: "Lobby".to_owned(),
                position: 1,
                visibility: "public".to_owned(),
            },
            RequestOptions::new(),
        )
        .await
        .expect("group");
    assert_eq!(group.data.id, id(101));

    let channel = client
        .channels()
        .update(
            id(102),
            UpdateChannelRequest {
                group_id: Some(None),
                pinned: Some(true),
                ..UpdateChannelRequest::default()
            },
            RequestOptions::new()
                .with_if_match(Etag::new("\"channel-1\"").expect("etag"))
                .with_parent_if_match(Etag::new("\"group-1\"").expect("etag")),
        )
        .await
        .expect("channel");
    assert_eq!(channel.data.id, id(102));
}

#[tokio::test(flavor = "current_thread")]
async fn channel_acl_preserves_parent_precondition_and_parent_response_etag() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.header("if-match"), Some("\"channel-1\""));
        assert_eq!(
            request.header("x-zephyr-parent-if-match"),
            Some("\"group-1\"")
        );
        assert_eq!(
            serde_json::from_slice::<Value>(request.body().expect("ACL body")).expect("JSON"),
            serde_json::json!({
                "principal_type":"user",
                "user_id":"7",
                "grant_parent":true
            })
        );
        Ok(zephyrvox_http::HttpResponse::new(
            201,
            [
                ("etag".to_owned(), "\"channel-2\"".to_owned()),
                ("x-zephyr-parent-etag".to_owned(), "\"group-2\"".to_owned()),
            ],
            serde_json::to_vec(&serde_json::json!({
                "code":0,"message":"","data":{
                    "id":"8","principal_type":"user","user_id":"7",
                    "created_at":100
                }
            }))
            .expect("JSON"),
        ))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");
    let response = client
        .channels()
        .add_access(
            id(102),
            AccessPrincipal::User(id(7)),
            true,
            RequestOptions::new()
                .with_if_match(Etag::new("\"channel-1\"").expect("etag"))
                .with_parent_if_match(Etag::new("\"group-1\"").expect("etag")),
        )
        .await
        .expect("ACL");
    assert_eq!(response.data.id, id(8));
    assert_eq!(
        response.headers.etag.expect("etag").as_str(),
        "\"channel-2\""
    );
    assert_eq!(
        response.headers.parent_etag.expect("parent etag").as_str(),
        "\"group-2\""
    );
}

#[tokio::test(flavor = "current_thread")]
async fn resource_mutations_require_explicit_etag_preconditions() {
    let transport = FakeTransport::new(|_| panic!("precondition failure must be local"));
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");

    assert!(matches!(
        client
            .groups()
            .update(
                id(101),
                zephyrvox_http::UpdateGroupRequest::default(),
                RequestOptions::new(),
            )
            .await,
        Err(HttpError::MissingPrecondition { header: "If-Match" })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn stale_etag_is_exposed_as_a_typed_precondition_error() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.url().path(), "/api/v0/groups/101");
        Ok(support::api_error(412, 2, "precondition failed"))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    let result = client
        .groups()
        .update(
            id(101),
            UpdateGroupRequest {
                name: Some("Lobby".to_owned()),
                ..UpdateGroupRequest::default()
            },
            RequestOptions::new().with_if_match(Etag::new("\"group-1\"").expect("etag")),
        )
        .await;
    assert!(matches!(
        result,
        Err(HttpError::PreconditionFailed(error))
            if error.http_status == Some(412)
                && error.code == 2
                && error.endpoint.as_deref() == Some("/api/v0/groups/101")
    ));
}
