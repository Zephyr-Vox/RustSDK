mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::Value;
use support::{FakeTransport, api_error, envelope};
use url::Url;
use zephyrvox_http::{
    AccessToken, ActivateRequest, ApiClient, HttpMethod, HttpRequest, HttpResponse, LoginRequest,
    RefreshToken, RegisterRequest, RequestOptions, TokenPair,
};
use zephyrvox_wire::{IdempotencyKey, ServerCard, TransportScheme};

fn plain_card() -> ServerCard {
    ServerCard::new("example.test", 80, TransportScheme::Plain, None, None).expect("card")
}

fn tokens() -> TokenPair {
    TokenPair::new(
        AccessToken::new("access").expect("access"),
        RefreshToken::new("refresh").expect("refresh"),
        Duration::from_secs(300),
    )
    .expect("tokens")
}

#[tokio::test(flavor = "current_thread")]
async fn login_rotation_and_logout_keep_tokens_out_of_debug_output() {
    let transport = FakeTransport::new(|request| match request.url().path() {
        "/api/v0/auth/login" => Ok(envelope(serde_json::json!({
            "access_token": "access-one",
            "refresh_token": "refresh-one",
            "expires_in": 300,
            "user": {
                "id": "123",
                "username": "alice",
                "nickname": "Alice",
                "avatar": ""
            }
        }))),
        "/api/v0/auth/logout" => {
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("logout body"))
                    .expect("logout JSON"),
                serde_json::json!({"refresh_token": "refresh-one"})
            );
            assert!(request.header("authorization").is_none());
            Ok(zephyrvox_http::HttpResponse::empty(204))
        }
        path => panic!("unexpected path {path}"),
    });
    let client = ApiClient::with_memory_transport(plain_card(), Arc::new(transport.clone()))
        .expect("client");

    let result = client
        .auth()
        .login(LoginRequest {
            username: "alice".to_owned(),
            password: "secret".to_owned(),
            device_id: "test".to_owned(),
        })
        .await
        .expect("login");
    let debug = format!("{result:?}");
    assert!(!debug.contains("access-one"));
    assert!(!debug.contains("refresh-one"));
    assert_eq!(
        client
            .load_credentials()
            .await
            .expect("load")
            .expect("tokens")
            .access_token()
            .as_str(),
        "access-one"
    );

    client.auth().logout().await.expect("logout");
    assert!(client.load_credentials().await.expect("load").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_401s_share_one_refresh_rotation() {
    let refresh_count = Arc::new(AtomicUsize::new(0));
    let refresh_count_for_handler = Arc::clone(&refresh_count);
    let transport = FakeTransport::new(move |request| match request.url().path() {
        "/api/v0/state/snapshot" => {
            if request.header("authorization") == Some("Bearer fresh-access") {
                Ok(envelope(
                    serde_json::from_str(include_str!("fixtures/state-snapshot.json"))
                        .expect("snapshot fixture"),
                ))
            } else {
                Ok(api_error(401, 1002, "unauthorized"))
            }
        }
        "/api/v0/auth/refresh" => {
            refresh_count_for_handler.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                serde_json::from_slice::<Value>(request.body().expect("refresh body"))
                    .expect("refresh JSON"),
                serde_json::json!({"refresh_token": "old-refresh"})
            );
            Ok(envelope(serde_json::json!({
                "access_token": "fresh-access",
                "refresh_token": "fresh-refresh",
                "expires_in": 300
            })))
        }
        path => panic!("unexpected path {path}"),
    })
    .with_delay(Duration::from_millis(10));
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client
        .set_tokens(
            TokenPair::new(
                AccessToken::new("old-access").expect("access"),
                RefreshToken::new("old-refresh").expect("refresh"),
                Duration::from_secs(300),
            )
            .expect("tokens"),
        )
        .await
        .expect("store tokens");

    let (first, second) = tokio::join!(client.snapshot(), client.snapshot());
    first.expect("first snapshot");
    second.expect("second snapshot");
    assert_eq!(refresh_count.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_refresh_leader_wakes_waiters_and_allows_a_new_flight() {
    let refresh_count = Arc::new(AtomicUsize::new(0));
    let refresh_count_for_handler = Arc::clone(&refresh_count);
    let transport = FakeTransport::new(move |request| {
        assert_eq!(request.url().path(), "/api/v0/auth/refresh");
        refresh_count_for_handler.fetch_add(1, Ordering::SeqCst);
        Ok(envelope(serde_json::json!({
            "access_token": "fresh-access",
            "refresh_token": "fresh-refresh",
            "expires_in": 300
        })))
    })
    .with_delay(Duration::from_millis(100));
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client
        .set_tokens(
            TokenPair::new(
                AccessToken::new("old-access").expect("access"),
                RefreshToken::new("old-refresh").expect("refresh"),
                Duration::from_secs(300),
            )
            .expect("tokens"),
        )
        .await
        .expect("store tokens");

    let leader_client = client.clone();
    let leader = tokio::spawn(async move { leader_client.auth().refresh().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while refresh_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("refresh leader started");

    let waiter_client = client.clone();
    let waiter = tokio::spawn(async move { waiter_client.auth().refresh().await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    leader.abort();
    assert!(
        leader
            .await
            .expect_err("leader must be canceled")
            .is_cancelled()
    );

    let waiter_result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter must wake")
        .expect("waiter task");
    assert!(matches!(
        waiter_result,
        Err(zephyrvox_http::HttpError::AuthExpired)
    ));
    assert_eq!(refresh_count.load(Ordering::SeqCst), 1);

    client.auth().refresh().await.expect("new refresh flight");
    assert_eq!(refresh_count.load(Ordering::SeqCst), 2);
}

#[test]
fn mutation_options_generate_a_valid_stable_key() {
    let options = RequestOptions::mutation();
    let key = options.idempotency_key().expect("generated key");
    assert!((16..=64).contains(&key.as_str().len()));
    assert!(
        key.as_str()
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_refresh_expires_and_clears_local_credentials() {
    let transport = FakeTransport::new(|request| match request.url().path() {
        "/api/v0/state/snapshot" => Ok(api_error(401, 1002, "unauthorized")),
        "/api/v0/auth/refresh" => Ok(api_error(401, 1, "invalid refresh token")),
        path => panic!("unexpected path {path}"),
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    assert!(matches!(
        client.snapshot().await,
        Err(zephyrvox_http::HttpError::AuthExpired)
    ));
    assert!(client.load_credentials().await.expect("load").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn registration_defaults_the_card_invite_without_leaking_secrets_in_debug() {
    let card = ServerCard::new(
        "example.test",
        80,
        TransportScheme::Plain,
        None,
        Some("card-invite".to_owned()),
    )
    .expect("card");
    let transport = FakeTransport::new(|request| {
        assert_eq!(
            serde_json::from_slice::<Value>(request.body().expect("register body"))
                .expect("register JSON"),
            serde_json::json!({
                "username": "alice",
                "password": "secret",
                "invite": "card-invite"
            })
        );
        assert_eq!(
            request.header("idempotency-key"),
            Some("registration-key-1")
        );
        Ok(envelope(serde_json::json!({
            "user": {
                "id": "123",
                "username": "alice",
                "nickname": "Alice",
                "avatar": ""
            }
        })))
    });
    let client = ApiClient::with_memory_transport(card, Arc::new(transport)).expect("client");
    let input = RegisterRequest {
        username: "alice".to_owned(),
        password: "secret".to_owned(),
        nickname: None,
        invite: None,
    };
    let debug = format!("{input:?}");
    assert!(!debug.contains("card-invite"));
    let result = client
        .auth()
        .register_with_options(
            input,
            RequestOptions::new().with_idempotency_key(
                IdempotencyKey::new("registration-key-1").expect("idempotency key"),
            ),
        )
        .await
        .expect("register");
    assert_eq!(result.user.avatar, None);
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_response_variants_preserve_state_headers() {
    let transport = FakeTransport::new(|request| {
        let data = serde_json::json!({
            "user": {
                "id": "123",
                "username": "alice",
                "nickname": "Alice",
                "avatar": ""
            }
        });
        let (status, command_id) = match request.url().path() {
            "/api/v0/auth/register" => (201, "register-command"),
            "/api/v0/admin/activate" => (200, "activate-command"),
            path => panic!("unexpected path {path}"),
        };
        Ok(HttpResponse::new(
            status,
            [("x-zephyr-command-id".to_owned(), command_id.to_owned())],
            serde_json::to_vec(&serde_json::json!({
                "code": 0,
                "message": "",
                "data": data,
            }))
            .expect("JSON"),
        ))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");

    let registered = client
        .auth()
        .register_response(RegisterRequest {
            username: "alice".to_owned(),
            password: "secret".to_owned(),
            nickname: None,
            invite: None,
        })
        .await
        .expect("register");
    assert_eq!(
        registered.headers.command_id.as_deref(),
        Some("register-command")
    );

    let activated = client
        .auth()
        .activate_response(ActivateRequest {
            code: "activation".to_owned(),
            username: "alice".to_owned(),
            password: "secret".to_owned(),
            nickname: None,
        })
        .await
        .expect("activate");
    assert_eq!(
        activated.headers.command_id.as_deref(),
        Some("activate-command")
    );
}

#[test]
fn transport_request_and_response_debug_redact_credentials() {
    let mut request = HttpRequest::new(
        HttpMethod::Post,
        Url::parse("https://example.test/api/v0/auth/login").expect("URL"),
    );
    request
        .set_header("authorization", "Bearer access-secret")
        .expect("header");
    request.set_body("refresh-secret");
    let request_debug = format!("{request:?}");
    assert!(!request_debug.contains("access-secret"));
    assert!(!request_debug.contains("refresh-secret"));

    let userinfo_request = HttpRequest::new(
        HttpMethod::Get,
        Url::parse("https://user:refresh-secret@example.test/api/v0").expect("URL"),
    );
    assert!(!format!("{userinfo_request:?}").contains("refresh-secret"));

    assert!(matches!(
        request.set_header("invalid:name", "value"),
        Err(zephyrvox_http::HttpError::InvalidHeader(_))
    ));

    let response = HttpResponse::new(
        200,
        [("set-cookie".to_owned(), "refresh=refresh-secret".to_owned())],
        "access-secret",
    );
    let response_debug = format!("{response:?}");
    assert!(!response_debug.contains("access-secret"));
    assert!(!response_debug.contains("refresh-secret"));
}
