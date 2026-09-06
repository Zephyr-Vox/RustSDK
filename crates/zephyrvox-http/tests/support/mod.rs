#![allow(dead_code)]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use tokio::time::sleep;
use zephyrvox_http::{BoxFuture, HttpError, HttpRequest, HttpResponse, HttpTransport};

pub type Handler = dyn Fn(&HttpRequest) -> Result<HttpResponse, HttpError> + Send + Sync + 'static;

#[derive(Clone)]
pub struct FakeTransport {
    handler: Arc<Handler>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    delay: Duration,
}

impl FakeTransport {
    pub fn new(
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, HttpError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            requests: Arc::new(Mutex::new(Vec::new())),
            delay: Duration::ZERO,
        }
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("request lock").clone()
    }
}

impl HttpTransport for FakeTransport {
    fn send(&self, request: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
        self.requests
            .lock()
            .expect("request lock")
            .push(request.clone());
        let response = (self.handler)(&request);
        let delay = self.delay;
        Box::pin(async move {
            if !delay.is_zero() {
                sleep(delay).await;
            }
            response
        })
    }
}

pub fn json_response(value: serde_json::Value) -> HttpResponse {
    HttpResponse::new(
        200,
        [("content-type".to_owned(), "application/json".to_owned())],
        Bytes::from(serde_json::to_vec(&value).expect("JSON")),
    )
}

pub fn envelope(data: serde_json::Value) -> HttpResponse {
    json_response(serde_json::json!({
        "code": 0,
        "message": "",
        "data": data
    }))
}

pub fn api_error(status: u16, code: i32, message: &str) -> HttpResponse {
    HttpResponse::new(
        status,
        [("content-type".to_owned(), "application/json".to_owned())],
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "code": code,
                "message": message,
                "data": null
            }))
            .expect("JSON"),
        ),
    )
}
