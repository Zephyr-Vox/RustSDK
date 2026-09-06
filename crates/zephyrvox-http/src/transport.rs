use std::time::Duration;

use crate::{
    BoxFuture,
    errors::HttpError,
    request::{HttpMethod, HttpRequest, HttpResponse},
    tls_pin::{TlsVerificationFailure, pinned_client_config},
};
use zephyrvox_wire::ServerCard;

/// The transport boundary used by [`crate::ApiClient`] and fake HTTP tests.
///
/// Implementations must send the request exactly as supplied and return the
/// complete response body.  The trait is object-safe so an application can
/// inject a test server, an instrumented transport, or a different async HTTP
/// implementation.  Implementations are shared across client clones and must
/// therefore be safe for concurrent calls.
pub trait HttpTransport: Send + Sync {
    /// Sends one fully formed request and returns its complete response.
    ///
    /// The returned future is owned by the caller and may outlive the borrow of
    /// the transport only for as long as the transport itself remains alive.
    /// Authentication, refresh, response-envelope decoding, and endpoint
    /// validation are handled by [`crate::ApiClient`], not by this trait.
    fn send(&self, request: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>>;
}

/// The production Tokio/reqwest HTTP transport.
///
/// A transport is bound to one [`ServerCard`].  Every request must use the
/// card's exact scheme, host, and port and must not contain URL userinfo or a
/// fragment.  TLS cards additionally install a certificate verifier that
/// checks hostname, validity, and the card's SPKI fingerprint during the
/// handshake. Redirects are disabled so a server cannot move a request away
/// from the card's authority or downgrade its transport policy. There is no
/// insecure certificate-bypass mode.
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
    expected_scheme: &'static str,
    expected_host: String,
    expected_port: u16,
    pin_failure: Option<std::sync::Arc<std::sync::Mutex<Option<TlsVerificationFailure>>>>,
}

impl ReqwestTransport {
    /// Creates a transport whose TLS mode follows the server card.
    ///
    /// `timeout` is passed to reqwest as the whole-request timeout.  Plain
    /// cards use ordinary HTTP.  TLS cards use the pinned rustls verifier and
    /// therefore may connect to a self-signed CommunityServer certificate only
    /// when its SPKI fingerprint matches the card.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Transport`] if the TLS/HTTP client cannot be built.
    pub fn new(card: &ServerCard, timeout: Duration) -> Result<Self, HttpError> {
        let mut builder = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none());
        let pin_failure = if let Some(fingerprint) = card.fingerprint() {
            let (config, failure) = pinned_client_config(fingerprint)?;
            builder = builder.use_preconfigured_tls(config);
            Some(failure)
        } else {
            None
        };
        let client = builder
            .build()
            .map_err(|error| HttpError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            expected_scheme: if card.scheme().is_tls() {
                "https"
            } else {
                "http"
            },
            expected_host: card.host().to_owned(),
            expected_port: card.port(),
            pin_failure,
        })
    }

    async fn send_inner(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        if request.url().scheme() != self.expected_scheme
            || request.url().host_str() != Some(self.expected_host.as_str())
            || request.url().port_or_known_default() != Some(self.expected_port)
            || !request.url().username().is_empty()
            || request.url().password().is_some()
            || request.url().fragment().is_some()
        {
            return Err(HttpError::InvalidUrl(
                "request URL does not match the server card".to_owned(),
            ));
        }
        let mut builder = self
            .client
            .request(request.method().into(), request.url().clone());
        for (name, value) in request.headers() {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body() {
            builder = builder.body(body.clone());
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => return Err(self.map_transport_error(error)),
        };

        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                let value = value
                    .to_str()
                    .map_err(|error| HttpError::InvalidHeader(error.to_string()))?;
                Ok((name.as_str().to_owned(), value.to_owned()))
            })
            .collect::<Result<Vec<_>, HttpError>>()?;
        let body = response
            .bytes()
            .await
            .map_err(|error| HttpError::Transport(error.to_string()))?;
        Ok(HttpResponse::new(status, headers, body))
    }

    /// Maps a rustls validation failure to the public SDK error contract.
    fn map_transport_error(&self, error: reqwest::Error) -> HttpError {
        let Some(failure) = &self.pin_failure else {
            return HttpError::Transport(error.to_string());
        };
        let recorded = failure.lock().ok().and_then(|mut value| value.take());
        match recorded {
            Some(TlsVerificationFailure::PinMismatch { expected, actual }) => {
                HttpError::TlsPinMismatch {
                    expected: expected.to_hex(),
                    actual: actual.to_hex(),
                }
            }
            Some(TlsVerificationFailure::InvalidCertificate) => {
                HttpError::TlsPeerCertificateInvalid
            }
            None => HttpError::Transport(error.to_string()),
        }
    }
}

impl HttpTransport for ReqwestTransport {
    fn send(&self, request: HttpRequest) -> BoxFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(self.send_inner(request))
    }
}

impl From<HttpMethod> for reqwest::Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::Get => Self::GET,
            HttpMethod::Post => Self::POST,
            HttpMethod::Patch => Self::PATCH,
            HttpMethod::Put => Self::PUT,
            HttpMethod::Delete => Self::DELETE,
        }
    }
}
