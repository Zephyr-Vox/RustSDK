use std::{sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{Mutex, OnceCell, RwLock};
use url::Url;

use crate::{
    auth::AuthApi,
    credentials::{CredentialStore, MemoryCredentialStore, TokenPair},
    errors::HttpError,
    request::{HttpMethod, HttpRequest, RequestOptions},
    response::{ApiResponse, decode_json, decode_no_content},
    transport::{HttpTransport, ReqwestTransport},
};
use zephyrvox_wire::{AUTHORIZATION, ServerCard};

mod refresh;

#[derive(Clone, Copy)]
pub(crate) enum AuthRequirement {
    None,
    AccessToken { retry_once: bool },
}

/// A clonable, concurrency-safe HTTP client for the CommunityServer control plane.
///
/// `ApiClient` owns the server-card transport policy and shares credential and
/// refresh state across clones.  Protected requests load the injected
/// [`CredentialStore`], add the current bearer token, and retry one `401`
/// response through the refresh singleflight.  A refresh rotation is persisted
/// before it becomes the active pair.  The default [`ApiClient::new`] constructor
/// keeps credentials only in memory; applications that need persistence should
/// use [`ApiClient::with_transport`] with their own store.
#[derive(Clone)]
pub struct ApiClient {
    card: ServerCard,
    base_url: Url,
    transport: Arc<dyn HttpTransport>,
    credential_store: Arc<dyn CredentialStore>,
    token_state: Arc<RwLock<Option<TokenPair>>>,
    credentials_loaded: Arc<OnceCell<()>>,
    credential_gate: Arc<Mutex<()>>,
    refresh_gate: Arc<Mutex<Option<Arc<refresh::RefreshFlight>>>>,
}

impl ApiClient {
    /// Creates a production client using the server-card transport policy and an
    /// in-memory credential store.
    ///
    /// For a TLS card, the transport validates the peer certificate's server
    /// name and validity period and requires its SPKI SHA-256 fingerprint to
    /// match the card.  The `timeout` applies to each reqwest operation.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::InvalidUrl`] when the card cannot form the control
    /// URL, or [`HttpError::Transport`] when the reqwest/TLS client cannot be
    /// constructed.
    pub fn new(card: ServerCard, timeout: Duration) -> Result<Self, HttpError> {
        let transport = Arc::new(ReqwestTransport::new(&card, timeout)?);
        Self::with_transport(card, transport, Arc::new(MemoryCredentialStore::new()))
    }

    /// Creates a client around a custom transport and credential store.
    ///
    /// The transport and store are shared by all clones of the returned client.
    /// Credential loads and saves are serialized so concurrent requests cannot
    /// overwrite a newer token pair with a stale load.  This constructor is the
    /// seam used by fake HTTP and interoperability tests; production callers
    /// normally use [`ApiClient::new`].
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::InvalidUrl`] if the card cannot form the control
    /// base URL.
    pub fn with_transport(
        card: ServerCard,
        transport: Arc<dyn HttpTransport>,
        credential_store: Arc<dyn CredentialStore>,
    ) -> Result<Self, HttpError> {
        let base_url = control_base_url(&card)?;
        Ok(Self {
            card,
            base_url,
            transport,
            credential_store,
            token_state: Arc::new(RwLock::new(None)),
            credentials_loaded: Arc::new(OnceCell::new()),
            credential_gate: Arc::new(Mutex::new(())),
            refresh_gate: Arc::new(Mutex::new(None)),
        })
    }

    /// Creates a client around a custom transport and memory credentials.
    ///
    /// This is useful for tests and for applications whose token lifecycle is
    /// intentionally process-local.  All clones share the same memory store.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::InvalidUrl`] if the card cannot form the control
    /// base URL.
    pub fn with_memory_transport(
        card: ServerCard,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<Self, HttpError> {
        Self::with_transport(card, transport, Arc::new(MemoryCredentialStore::new()))
    }

    /// Returns the server card that fixes this client's transport policy.
    ///
    /// The card's scheme, host, and port are immutable for the lifetime of this
    /// client; metadata discovery cannot downgrade TLS or redirect requests to
    /// another authority.
    pub fn server_card(&self) -> &ServerCard {
        &self.card
    }

    /// Loads the injected credential store into the active client state.
    ///
    /// The first successful call performs the store load.  Concurrent callers
    /// share the resulting in-memory pair, and later calls return a clone of
    /// that pair without loading the store again.  A failed load does not mark
    /// the store as initialized, so a later call may retry it.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Credential`] if the store cannot load its value.
    pub async fn load_credentials(&self) -> Result<Option<TokenPair>, HttpError> {
        let _gate = self.credential_gate.lock().await;
        if self.credentials_loaded.get().is_none() {
            let tokens = self.credential_store.load().await?;
            *self.token_state.write().await = tokens;
            let _ = self.credentials_loaded.set(());
        }
        Ok(self.token_state.read().await.clone())
    }

    /// Replaces and persists the active token pair.
    ///
    /// The pair becomes active only after [`CredentialStore::save`] succeeds;
    /// a failed save leaves the previously active pair unchanged.  The method
    /// is safe to call concurrently with requests and other token updates.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Credential`] if persistence fails.
    pub async fn set_tokens(&self, tokens: TokenPair) -> Result<(), HttpError> {
        let _gate = self.credential_gate.lock().await;
        self.credential_store.save(&tokens).await?;
        *self.token_state.write().await = Some(tokens);
        let _ = self.credentials_loaded.set(());
        Ok(())
    }

    /// Clears both the active and persisted credentials.
    ///
    /// The in-memory pair is removed only after [`CredentialStore::clear`]
    /// succeeds.  This ordering avoids silently losing a usable session when a
    /// durable store is temporarily unavailable.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Credential`] if persistence fails.
    pub async fn clear_tokens(&self) -> Result<(), HttpError> {
        let _gate = self.credential_gate.lock().await;
        self.credential_store.clear().await?;
        *self.token_state.write().await = None;
        let _ = self.credentials_loaded.set(());
        Ok(())
    }

    /// Returns the authentication API accessor.
    ///
    /// The accessor borrows this client and performs no network operation until
    /// one of its endpoint methods is called.
    pub fn auth(&self) -> AuthApi<'_> {
        AuthApi::new(self)
    }

    pub(crate) fn endpoint(&self, path: &str) -> Result<Url, HttpError> {
        self.base_url
            .join(path.trim_start_matches('/'))
            .map_err(|error| HttpError::InvalidUrl(error.to_string()))
    }

    pub(crate) fn empty_request(
        &self,
        method: HttpMethod,
        path: &str,
        options: RequestOptions,
    ) -> Result<HttpRequest, HttpError> {
        let mut request = HttpRequest::new(method, self.endpoint(path)?);
        options.apply(&mut request)?;
        Ok(request)
    }

    pub(crate) fn json_request<T: Serialize>(
        &self,
        method: HttpMethod,
        path: &str,
        body: &T,
        options: RequestOptions,
    ) -> Result<HttpRequest, HttpError> {
        let mut request = HttpRequest::new(method, self.endpoint(path)?);
        request.set_json(body)?;
        options.apply(&mut request)?;
        Ok(request)
    }

    pub(crate) async fn send_json<T: DeserializeOwned>(
        &self,
        request: HttpRequest,
        auth: AuthRequirement,
    ) -> Result<ApiResponse<T>, HttpError> {
        let endpoint = request.url().path().to_owned();
        let response = self.send_request(request, auth).await?;
        decode_json(response, &endpoint)
    }

    pub(crate) async fn send_no_content(
        &self,
        request: HttpRequest,
        auth: AuthRequirement,
    ) -> Result<ApiResponse<()>, HttpError> {
        let endpoint = request.url().path().to_owned();
        let response = self.send_request(request, auth).await?;
        decode_no_content(response, &endpoint)
    }

    async fn send_request(
        &self,
        request: HttpRequest,
        auth: AuthRequirement,
    ) -> Result<crate::request::HttpResponse, HttpError> {
        let (request, sent_access_token) = match auth {
            AuthRequirement::None => (request, None),
            AuthRequirement::AccessToken { .. } => {
                let tokens = self
                    .load_credentials()
                    .await?
                    .ok_or(HttpError::AuthenticationRequired)?;
                let access_token = tokens.access_token().clone();
                let mut request = request;
                request.set_header(AUTHORIZATION, &format!("Bearer {}", access_token.as_str()))?;
                (request, Some(access_token))
            }
        };
        let response = self.transport.send(request.clone()).await?;
        if response.status() == 401
            && matches!(auth, AuthRequirement::AccessToken { retry_once: true })
        {
            let current_access_token = self
                .load_credentials()
                .await?
                .map(|tokens| tokens.access_token().clone());
            if current_access_token == sent_access_token {
                self.refresh_access().await?;
            }
            let retry = self.with_access_token(request).await?;
            return self.transport.send(retry).await;
        }
        Ok(response)
    }

    async fn with_access_token(&self, mut request: HttpRequest) -> Result<HttpRequest, HttpError> {
        let tokens = self
            .load_credentials()
            .await?
            .ok_or(HttpError::AuthenticationRequired)?;
        request.set_header(
            AUTHORIZATION,
            &format!("Bearer {}", tokens.access_token().as_str()),
        )?;
        Ok(request)
    }
}

fn control_base_url(card: &ServerCard) -> Result<Url, HttpError> {
    let scheme = if card.scheme().is_tls() {
        "https"
    } else {
        "http"
    };
    let host = if card.host().contains(':') {
        format!("[{}]", card.host())
    } else {
        card.host().to_owned()
    };
    Url::parse(&format!("{scheme}://{host}:{}/api/v0/", card.port()))
        .map_err(|error| HttpError::InvalidUrl(error.to_string()))
}
