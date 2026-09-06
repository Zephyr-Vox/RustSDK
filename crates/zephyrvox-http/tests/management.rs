mod support;

use std::{sync::Arc, time::Duration};

use serde_json::Value;
use support::{FakeTransport, envelope};
use zephyrvox_http::{
    AccessToken, ApiClient, CreateBindingRequest, CreateMuteRequest, HttpMethod, ModerationScope,
    RbacScope, RequestOptions, TokenPair, UpdateMuteRequest, UpdateRoleRequest,
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
async fn rbac_and_moderation_requests_match_backend_id_encoding() {
    let transport = FakeTransport::new(|request| match (request.method(), request.url().path()) {
        (HttpMethod::Post, "/api/v0/rbac/bindings") => {
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("binding body"))
                    .expect("JSON"),
                serde_json::json!({
                    "user_id": 7,
                    "role_key": "member",
                    "scope": {"type":"group","id":11}
                })
            );
            Ok(envelope(serde_json::json!({
                "id": 12,
                "user_id": 7,
                "role_key": "member",
                "scope": {"type":"group","id":11},
                "created_at": 100
            })))
        }
        (HttpMethod::Post, "/api/v0/mutes") => {
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("mute body")).expect("JSON"),
                serde_json::json!({
                    "scope": {"type":"channel","id":"11"},
                    "user_id":"7",
                    "kind":"voice",
                    "reason":"test"
                })
            );
            Ok(envelope(serde_json::json!({
                "id":"13",
                "scope":{"type":"channel","id":"11"},
                "user_id":"7",
                "kind":"voice",
                "expires_at":null,
                "reason":"test",
                "created_at":100,
                "version":"1"
            })))
        }
        (HttpMethod::Patch, "/api/v0/mutes/13") => {
            assert_eq!(request.header("if-match"), Some("\"mute-1\""));
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("mute update body"))
                    .expect("JSON"),
                serde_json::json!({"expires_at":null})
            );
            Ok(envelope(serde_json::json!({
                "id":"13",
                "scope":{"type":"channel","id":"11"},
                "user_id":"7",
                "kind":"voice",
                "expires_at":null,
                "reason":"test",
                "created_at":100,
                "version":"2"
            })))
        }
        path => panic!("unexpected request {path:?}"),
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    let binding = client
        .rbac()
        .create_binding(
            CreateBindingRequest {
                user_id: id(7),
                role_key: "member".to_owned(),
                scope: RbacScope {
                    kind: "group".to_owned(),
                    id: Some(id(11)),
                },
            },
            RequestOptions::new(),
        )
        .await
        .expect("binding");
    assert_eq!(binding.data.id, id(12));

    let mute = client
        .moderation()
        .create(
            CreateMuteRequest {
                scope: ModerationScope {
                    kind: "channel".to_owned(),
                    id: Some(id(11)),
                },
                user_id: id(7),
                kind: "voice".to_owned(),
                expires_at: None,
                reason: Some("test".to_owned()),
            },
            RequestOptions::new(),
        )
        .await
        .expect("mute");
    assert_eq!(mute.data.id, id(13));

    let updated = client
        .moderation()
        .update(
            id(13),
            UpdateMuteRequest {
                expires_at: Some(None),
                reason: None,
            },
            RequestOptions::new().with_if_match(Etag::new("\"mute-1\"").expect("etag")),
        )
        .await
        .expect("mute update");
    assert_eq!(updated.data.version, "2");
}

#[tokio::test(flavor = "current_thread")]
async fn admin_metrics_decode_without_exposing_transport_specific_types() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.url().path(), "/api/v0/admin/metrics");
        Ok(envelope(serde_json::json!({
            "http_connections": 1,
            "websocket_connections": 2,
            "udp_sessions": 3,
            "udp": {
                "packets_received": 10,
                "dropped": 1,
                "global_dropped": 0,
                "source_dropped": 0,
                "session_dropped": 1,
                "send_success": 9,
                "send_errors": 0,
                "drop_ratio": 0.1,
                "global_drop_ratio": 0.0,
                "source_drop_ratio": 0.0,
                "session_drop_ratio": 0.1
            },
            "relay": {
                "enqueued": 10,
                "dropped_overload": 0,
                "dropped_soft_limit": 0,
                "dropped_no_authority": 0,
                "dropped_stale_authority": 0,
                "dropped_muted": 0,
                "dropped_no_membership": 0,
                "dropped_invalid_channel": 0,
                "sent": 9,
                "send_errors": 0,
                "queue": {
                    "items": 0,
                    "bytes": 0,
                    "item_capacity": 4096,
                    "byte_capacity": 16777216,
                    "utilization": 0.0,
                    "shard_items": [0,0,0,0,0,0,0,0],
                    "shard_bytes": [0,0,0,0,0,0,0,0],
                    "shard_utilization": [0,0,0,0,0,0,0,0]
                },
                "p95_work_latency_ms": 4
            },
            "relay_packets_per_second": 10.0,
            "relay_drop_ratio": 0.1,
            "load": {
                "hard": {"global_packets_per_sec": 1000, "session_packets_per_sec": 100},
                "soft": {"global_ingress_soft_pps": 900, "session_soft_pps": 90, "voice_stats_interval_ms": 5000},
                "overloaded": false,
                "recovery_ticks": 0,
                "last_input": {
                    "queue_utilization": 0.0,
                    "relay_drop_ratio": 0.0,
                    "udp_drop_ratio": 0.0,
                    "udp_global_drop_ratio": 0.0,
                    "udp_source_drop_ratio": 0.0,
                    "udp_session_drop_ratio": 0.0,
                    "relay_p95_latency_ms": 0,
                    "state_publication_lag_ms": 0
                }
            },
            "state_publication_latency_ms": 2,
            "snapshot_latency_ms": 3
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");
    let metrics = client.admin().metrics().await.expect("metrics");
    assert_eq!(metrics.data.udp_sessions, 3);
    assert_eq!(metrics.data.load.hard.session_packets_per_sec, 100);
    assert_eq!(metrics.data.relay.queue.item_capacity, 4096);
}

#[tokio::test(flavor = "current_thread")]
async fn role_paths_allow_builtin_updates_but_reject_path_traversal() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.url().path(), "/api/v0/rbac/roles/owner");
        Ok(envelope(serde_json::json!({
            "key": "owner",
            "display_name": "Founder",
            "rank": 1000000,
            "builtin": true,
            "immutable": true,
            "created_at": 1,
            "updated_at": 2,
            "version": 3
        })))
    });
    let client = ApiClient::with_memory_transport(plain_card(), Arc::new(transport.clone()))
        .expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    let role = client
        .rbac()
        .update_role(
            "owner",
            UpdateRoleRequest {
                display_name: Some("Founder".to_owned()),
                rank: None,
            },
            RequestOptions::new(),
        )
        .await
        .expect("owner update");
    assert_eq!(role.data.key, "owner");

    let error = client
        .rbac()
        .update_role(
            "../admin",
            UpdateRoleRequest::default(),
            RequestOptions::new(),
        )
        .await
        .expect_err("path traversal");
    assert!(matches!(error, zephyrvox_http::HttpError::Wire(_)));
    assert_eq!(transport.requests().len(), 1);
}
