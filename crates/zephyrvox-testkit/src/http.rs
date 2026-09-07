//! Deterministic HTTP transport and response helpers for integration tests.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use tokio::sync::Mutex as AsyncMutex;

use zephyrvox_http::{BoxFuture, HttpError, HttpRequest, HttpResponse, HttpTransport};

/// A FIFO HTTP transport fake.
///
/// Each call records the complete request value for assertions and consumes
/// one queued result. The fake is concurrency-safe and therefore exercises the
/// same shared-request behavior as the production
/// [`zephyrvox_http::ApiClient`]. Callers should avoid logging recorded bodies
/// because test requests may contain credentials.
#[derive(Clone, Default)]
pub struct FakeHttpTransport {
    responses: Arc<AsyncMutex<VecDeque<Result<HttpResponse, HttpError>>>>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl FakeHttpTransport {
    /// Creates an empty fake transport.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues one successful response at the back of the FIFO.
    pub async fn push_response(&self, response: HttpResponse) {
        self.responses.lock().await.push_back(Ok(response));
    }

    /// Queues one transport error at the back of the FIFO.
    pub async fn push_error(&self, error: HttpError) {
        self.responses.lock().await.push_back(Err(error));
    }

    /// Returns all requests observed so far in send order.
    pub fn requests(&self) -> Vec<HttpRequest> {
        self.requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Returns a JSON response with an arbitrary HTTP status.
    pub fn json_response(status: u16, value: serde_json::Value) -> HttpResponse {
        HttpResponse::new(
            status,
            [("content-type".to_owned(), "application/json".to_owned())],
            serde_json::to_vec(&value).expect("JSON response helper serialization"),
        )
    }

    /// Returns a successful CommunityServer JSON envelope.
    pub fn envelope(data: serde_json::Value) -> HttpResponse {
        Self::json_response(
            200,
            serde_json::json!({"code": 0, "message": "", "data": data}),
        )
    }

    /// Returns a structured CommunityServer API error response.
    pub fn api_error(status: u16, code: i32, message: &str) -> HttpResponse {
        Self::json_response(
            status,
            serde_json::json!({"code": code, "message": message, "data": null}),
        )
    }
}

impl HttpTransport for FakeHttpTransport {
    fn send(&self, request: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
        let responses = Arc::clone(&self.responses);
        let requests = Arc::clone(&self.requests);
        Box::pin(async move {
            requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request);
            responses.lock().await.pop_front().unwrap_or_else(|| {
                Err(HttpError::Transport(
                    "fake HTTP response queue is empty".to_owned(),
                ))
            })
        })
    }
}
