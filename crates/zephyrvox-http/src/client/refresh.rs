use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};

use super::ApiClient;
use crate::{
    credentials::TokenPair,
    errors::HttpError,
    request::{HttpMethod, RequestOptions},
    response::{ApiResponse, decode_json},
};

pub(super) struct RefreshFlight {
    result: Mutex<Option<Result<TokenPair, HttpError>>>,
    notify: Notify,
    cancelled: AtomicBool,
}

struct RefreshLeaderGuard {
    flight: Arc<RefreshFlight>,
    finished: bool,
}

impl Drop for RefreshLeaderGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.flight.cancelled.store(true, Ordering::Release);
            self.flight.notify.notify_waiters();
        }
    }
}

/// Allocates the shared state for one refresh singleflight.
fn new_refresh_flight() -> Arc<RefreshFlight> {
    Arc::new(RefreshFlight {
        result: Mutex::new(None),
        notify: Notify::new(),
        cancelled: AtomicBool::new(false),
    })
}

impl ApiClient {
    pub(crate) async fn refresh_access(&self) -> Result<TokenPair, HttpError> {
        let (flight, leader) = {
            let mut gate = self.refresh_gate.lock().await;
            if let Some(flight) = gate.as_ref() {
                if flight.cancelled.load(Ordering::Acquire) {
                    let flight = new_refresh_flight();
                    *gate = Some(Arc::clone(&flight));
                    (flight, true)
                } else {
                    (Arc::clone(flight), false)
                }
            } else {
                let flight = new_refresh_flight();
                *gate = Some(Arc::clone(&flight));
                (flight, true)
            }
        };

        if leader {
            let mut leader_guard = RefreshLeaderGuard {
                flight: Arc::clone(&flight),
                finished: false,
            };
            let result = self.refresh_once().await;
            *flight.result.lock().await = Some(result.clone());
            flight.notify.notify_waiters();
            leader_guard.finished = true;
            let mut gate = self.refresh_gate.lock().await;
            if gate
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &flight))
            {
                *gate = None;
            }
            return result;
        }

        let notified = flight.notify.notified();
        if flight.cancelled.load(Ordering::Acquire) {
            return Err(HttpError::AuthExpired);
        }
        if let Some(result) = flight.result.lock().await.clone() {
            return result;
        }
        notified.await;
        flight
            .result
            .lock()
            .await
            .clone()
            .unwrap_or(Err(HttpError::AuthExpired))
    }

    async fn refresh_once(&self) -> Result<TokenPair, HttpError> {
        let current = self
            .load_credentials()
            .await?
            .ok_or(HttpError::AuthenticationRequired)?;
        let body = RefreshRequest {
            refresh_token: current.refresh_token().as_str().to_owned(),
        };
        let request = self.json_request(
            HttpMethod::Post,
            "auth/refresh",
            &body,
            RequestOptions::new(),
        )?;
        let response = match self.transport.send(request).await {
            Ok(response) => response,
            Err(error) => return self.superseded_or_error(&current, error).await,
        };
        let endpoint = "/api/v0/auth/refresh";
        let decoded: ApiResponse<RefreshResponse> = match decode_json(response, endpoint) {
            Ok(decoded) => decoded,
            Err(error) if is_unauthorized(&error) => {
                return match self.expire_if_current(&current).await? {
                    Some(tokens) => Ok(tokens),
                    None => Err(HttpError::AuthExpired),
                };
            }
            Err(error) => return self.superseded_or_error(&current, error).await,
        };
        let tokens = match TokenPair::from_expires_in(
            decoded.data.access_token,
            decoded.data.refresh_token,
            decoded.data.expires_in,
        ) {
            Ok(tokens) => tokens,
            Err(error) => return self.superseded_or_error(&current, error.into()).await,
        };
        self.replace_if_current(&current, tokens).await
    }

    /// Keeps a newer login pair authoritative when an older refresh loses a
    /// race with that login; otherwise returns the original refresh error.
    async fn superseded_or_error(
        &self,
        expected: &TokenPair,
        error: HttpError,
    ) -> Result<TokenPair, HttpError> {
        let _gate = self.credential_gate.lock().await;
        if let Some(tokens) = self.token_state.read().await.clone()
            && &tokens != expected
        {
            return Ok(tokens);
        }
        Err(error)
    }

    /// Clears credentials only when the pair used by this refresh is still
    /// active.  A concurrent login wins and remains available to the caller.
    async fn expire_if_current(
        &self,
        expected: &TokenPair,
    ) -> Result<Option<TokenPair>, HttpError> {
        let _gate = self.credential_gate.lock().await;
        let active = self.token_state.read().await.clone();
        if active.as_ref() != Some(expected) {
            return Ok(active);
        }
        self.credential_store.clear().await?;
        *self.token_state.write().await = None;
        let _ = self.credentials_loaded.set(());
        Ok(None)
    }

    /// Commits a rotated pair only when the pair that initiated refresh is
    /// still active, preventing an older refresh from overwriting a login.
    async fn replace_if_current(
        &self,
        expected: &TokenPair,
        replacement: TokenPair,
    ) -> Result<TokenPair, HttpError> {
        let _gate = self.credential_gate.lock().await;
        let active = self.token_state.read().await.clone();
        if active.as_ref() != Some(expected) {
            return active.ok_or(HttpError::AuthenticationRequired);
        }
        self.credential_store.save(&replacement).await?;
        *self.token_state.write().await = Some(replacement.clone());
        let _ = self.credentials_loaded.set(());
        Ok(replacement)
    }
}

/// Reports whether a refresh response proves that the local session expired.
fn is_unauthorized(error: &HttpError) -> bool {
    match error {
        HttpError::Api(error) => error.http_status == Some(401),
        HttpError::HttpStatus { status, .. } => *status == 401,
        _ => false,
    }
}

#[derive(Serialize)]
struct RefreshRequest {
    refresh_token: String,
}

#[derive(Deserialize)]
struct RefreshResponse {
    access_token: String,
    refresh_token: String,
    expires_in: i64,
}
