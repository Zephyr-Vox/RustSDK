mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use support::{api_error, envelope};
use tokio::sync::Notify;
use zephyrvox_http::{
    AccessToken, ApiClient, BoxFuture, HttpError, HttpRequest, HttpResponse, HttpTransport,
    LoginRequest, RefreshToken, TokenPair,
};
use zephyrvox_wire::{ServerCard, TransportScheme};

#[derive(Clone)]
struct RefreshRaceTransport {
    refresh_started: Arc<Notify>,
    release_refresh: Arc<Notify>,
    snapshot_requests: Arc<AtomicUsize>,
    refresh_response: HttpResponse,
}

impl HttpTransport for RefreshRaceTransport {
    fn send(&self, request: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
        let path = request.url().path().to_owned();
        let authorization = request.header("authorization").map(str::to_owned);
        let refresh_started = Arc::clone(&self.refresh_started);
        let release_refresh = Arc::clone(&self.release_refresh);
        let snapshot_requests = Arc::clone(&self.snapshot_requests);
        let refresh_response = self.refresh_response.clone();
        Box::pin(async move {
            match path.as_str() {
                "/api/v0/state/snapshot" => {
                    let request_number = snapshot_requests.fetch_add(1, Ordering::SeqCst);
                    if request_number == 0 {
                        Ok(api_error(401, 1002, "unauthorized"))
                    } else {
                        assert_eq!(authorization.as_deref(), Some("Bearer new-access"));
                        Ok(envelope(
                            serde_json::from_str(include_str!("fixtures/state-snapshot.json"))
                                .expect("snapshot fixture"),
                        ))
                    }
                }
                "/api/v0/auth/refresh" => {
                    refresh_started.notify_waiters();
                    release_refresh.notified().await;
                    Ok(refresh_response)
                }
                "/api/v0/auth/login" => Ok(envelope(serde_json::json!({
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 300,
                    "user": {
                        "id": "123",
                        "username": "alice",
                        "nickname": "Alice",
                        "avatar": ""
                    }
                }))),
                path => panic!("unexpected path {path}"),
            }
        })
    }
}

fn plain_card() -> ServerCard {
    ServerCard::new("example.test", 80, TransportScheme::Plain, None, None).expect("card")
}

fn old_tokens() -> TokenPair {
    TokenPair::new(
        AccessToken::new("old-access").expect("access"),
        RefreshToken::new("old-refresh").expect("refresh"),
        Duration::from_secs(300),
    )
    .expect("tokens")
}

async fn run_login_wins_over_refresh(refresh_response: HttpResponse) {
    let transport = RefreshRaceTransport {
        refresh_started: Arc::new(Notify::new()),
        release_refresh: Arc::new(Notify::new()),
        snapshot_requests: Arc::new(AtomicUsize::new(0)),
        refresh_response,
    };
    let client = ApiClient::with_memory_transport(plain_card(), Arc::new(transport.clone()))
        .expect("client");
    client.set_tokens(old_tokens()).await.expect("old tokens");

    let refresh_started = transport.refresh_started.notified();
    let snapshot_client = client.clone();
    let snapshot = tokio::spawn(async move { snapshot_client.snapshot().await });
    tokio::time::timeout(Duration::from_secs(1), refresh_started)
        .await
        .expect("refresh request must start");

    client
        .auth()
        .login(LoginRequest {
            username: "alice".to_owned(),
            password: "secret".to_owned(),
            device_id: "race-test".to_owned(),
        })
        .await
        .expect("login");
    transport.release_refresh.notify_waiters();

    tokio::time::timeout(Duration::from_secs(1), snapshot)
        .await
        .expect("snapshot must finish")
        .expect("snapshot task")
        .expect("snapshot must retry with the login pair");
    let active = client
        .load_credentials()
        .await
        .expect("credentials")
        .expect("active credentials");
    assert_eq!(active.access_token().as_str(), "new-access");
    assert_eq!(active.refresh_token().as_str(), "new-refresh");
    assert_eq!(transport.snapshot_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn successful_refresh_cannot_overwrite_a_concurrent_login() {
    run_login_wins_over_refresh(envelope(serde_json::json!({
        "access_token": "stale-refresh-access",
        "refresh_token": "stale-refresh-token",
        "expires_in": 300
    })))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_refresh_cannot_clear_a_concurrent_login() {
    run_login_wins_over_refresh(api_error(401, 1, "invalid refresh token")).await;
}
