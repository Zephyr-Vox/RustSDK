use std::{
    collections::BTreeMap,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use serde::Serialize;
use url::Url;

use crate::errors::HttpError;
use zephyrvox_types::{ControlConnectionId, Etag};
use zephyrvox_wire::IdempotencyKey;

/// HTTP methods used by the current CommunityServer API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PATCH.
    Patch,
    /// PUT.
    Put,
    /// DELETE.
    Delete,
}

/// One transport-neutral HTTP request.
///
/// Requests own their body and normalized lowercase header map, so a request
/// can be cloned for an authenticated retry.  The [`Debug`](std::fmt::Debug)
/// implementation reports body length and redacts credential-like headers; it
/// never prints body contents.
#[derive(Clone)]
pub struct HttpRequest {
    method: HttpMethod,
    url: Url,
    headers: BTreeMap<String, String>,
    body: Option<Bytes>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &redacted_url(&self.url))
            .field("headers", &RedactedHeaders(&self.headers))
            .field("body_len", &self.body.as_ref().map(bytes::Bytes::len))
            .finish()
    }
}

impl HttpRequest {
    /// Creates an empty request.
    pub fn new(method: HttpMethod, url: Url) -> Self {
        Self {
            method,
            url,
            headers: BTreeMap::new(),
            body: None,
        }
    }

    /// Returns the HTTP method.
    pub const fn method(&self) -> HttpMethod {
        self.method
    }

    /// Returns the fully resolved URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the raw body bytes, if present.
    pub fn body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    /// Serializes `value` as JSON, replaces the body, and sets
    /// `Content-Type: application/json`.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Json`] when serialization fails.  The request is
    /// left unchanged in that case.
    pub fn set_json<T: Serialize>(&mut self, value: &T) -> Result<(), HttpError> {
        let body = serde_json::to_vec(value).map_err(|error| HttpError::Json(error.to_string()))?;
        self.body = Some(Bytes::from(body));
        self.set_header("content-type", "application/json")
    }

    /// Replaces the raw request body without changing the headers.
    pub fn set_body(&mut self, body: impl Into<Bytes>) {
        self.body = Some(body.into());
    }

    /// Inserts or replaces a single request header.
    ///
    /// Header names are normalized to lowercase.  Names must use HTTP token
    /// characters and values must not contain CR, LF, or other control bytes;
    /// horizontal tab remains permitted by the HTTP field-value grammar.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::InvalidHeader`] when either argument violates that
    /// grammar.  No header is inserted on error.
    pub fn set_header(&mut self, name: &str, value: &str) -> Result<(), HttpError> {
        if name.is_empty()
            || value
                .bytes()
                .any(|byte| (byte < 0x20 && byte != b'\t') || byte == 0x7f)
            || !name.bytes().all(is_http_token_byte)
        {
            return Err(HttpError::InvalidHeader(name.to_owned()));
        }
        self.headers
            .insert(name.to_ascii_lowercase(), value.to_owned());
        Ok(())
    }

    /// Returns a request header case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// Returns all normalized request headers.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
}

/// One transport-neutral HTTP response.
///
/// The body is retained as raw bytes until an endpoint decoder selects its
/// expected envelope type.  Its [`Debug`](std::fmt::Debug) implementation
/// reports only body length and redacts credential-like response headers.
#[derive(Clone)]
pub struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Bytes,
}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("headers", &RedactedHeaders(&self.headers))
            .field("body_len", &self.body.len())
            .finish()
    }
}

struct RedactedHeaders<'a>(&'a BTreeMap<String, String>);

impl fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = formatter.debug_map();
        for (name, value) in self.0 {
            if is_sensitive_header(name) {
                map.entry(name, &"<redacted>");
            } else {
                map.entry(name, value);
            }
        }
        map.finish()
    }
}

fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name,
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie" | "x-api-key"
    ) || name.contains("token")
        || name.contains("secret")
        || name.contains("password")
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'..=b'\'' | b'*'..=b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|'
                | b'~'
        )
}

fn redacted_url(url: &Url) -> String {
    let mut url = url.clone();
    if !url.username().is_empty() {
        let _ = url.set_username("<redacted>");
    }
    if url.password().is_some() {
        let _ = url.set_password(Some("<redacted>"));
    }
    url.to_string()
}

impl HttpResponse {
    /// Creates a response for a fake transport or adapter.
    ///
    /// Header names are normalized to lowercase.  Duplicate names are folded
    /// with the last supplied value winning.  The constructor is intended for
    /// already-normalized adapter responses rather than as a general-purpose
    /// multi-value header container.
    pub fn new(
        status: u16,
        headers: impl IntoIterator<Item = (String, String)>,
        body: impl Into<Bytes>,
    ) -> Self {
        let mut normalized = BTreeMap::new();
        for (name, value) in headers {
            normalized.insert(name.to_ascii_lowercase(), value);
        }
        Self {
            status,
            headers: normalized,
            body: body.into(),
        }
    }

    /// Creates an empty response.
    pub fn empty(status: u16) -> Self {
        Self::new(status, [], Bytes::new())
    }

    /// Returns the HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the raw body bytes.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Returns a response header case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// Returns all normalized response headers.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
}

/// Optional headers attached to a mutation request.
///
/// Endpoint methods add an idempotency key automatically for mutations when
/// the caller has not supplied one.  Resource mutations that require
/// optimistic concurrency validate that `If-Match` is present locally, and
/// channel-parent ACL grants similarly require the parent precondition.
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    idempotency_key: Option<IdempotencyKey>,
    if_match: Option<Etag>,
    parent_if_match: Option<Etag>,
    control_connection: Option<ControlConnectionId>,
}

impl RequestOptions {
    /// Creates empty request options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates mutation options with a generated idempotency key.
    ///
    /// The generated value is process-local and valid for one logical request;
    /// callers must provide a stable key with [`Self::with_idempotency_key`]
    /// when they may replay a request after an unknown network outcome.
    pub fn mutation() -> Self {
        Self {
            idempotency_key: Some(generate_idempotency_key()),
            ..Self::default()
        }
    }

    /// Sets a caller-owned stable idempotency key.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Sets the resource ETag precondition.
    pub fn with_if_match(mut self, etag: Etag) -> Self {
        self.if_match = Some(etag);
        self
    }

    /// Sets the parent resource ETag precondition.
    pub fn with_parent_if_match(mut self, etag: Etag) -> Self {
        self.parent_if_match = Some(etag);
        self
    }

    /// Binds a mutation to the active WebSocket control connection.
    pub fn with_control_connection(mut self, connection: ControlConnectionId) -> Self {
        self.control_connection = Some(connection);
        self
    }

    /// Returns the configured idempotency key.
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    /// Ensures that a mutation has a valid retry identity.
    pub(crate) fn ensure_idempotency(&mut self) {
        if self.idempotency_key.is_none() {
            self.idempotency_key = Some(generate_idempotency_key());
        }
    }

    /// Reports whether this request is bound to an active control connection.
    pub(crate) const fn has_control_connection(&self) -> bool {
        self.control_connection.is_some()
    }

    /// Verifies that a resource mutation carries its current ETag.
    pub(crate) fn require_if_match(&self) -> Result<(), HttpError> {
        if self.if_match.is_some() {
            Ok(())
        } else {
            Err(HttpError::MissingPrecondition {
                header: zephyrvox_wire::IF_MATCH,
            })
        }
    }

    /// Verifies that a channel-parent mutation carries its parent ETag.
    pub(crate) fn require_parent_if_match(&self) -> Result<(), HttpError> {
        if self.parent_if_match.is_some() {
            Ok(())
        } else {
            Err(HttpError::MissingPrecondition {
                header: zephyrvox_wire::PARENT_IF_MATCH,
            })
        }
    }

    pub(crate) fn apply(&self, request: &mut HttpRequest) -> Result<(), HttpError> {
        if let Some(key) = &self.idempotency_key {
            request.set_header(zephyrvox_wire::IDEMPOTENCY_KEY, key.as_str())?;
        }
        if let Some(etag) = &self.if_match {
            request.set_header(zephyrvox_wire::IF_MATCH, etag.as_str())?;
        }
        if let Some(etag) = &self.parent_if_match {
            request.set_header(zephyrvox_wire::PARENT_IF_MATCH, etag.as_str())?;
        }
        if let Some(connection) = &self.control_connection {
            request.set_header(zephyrvox_wire::CONTROL_CONNECTION, &connection.to_string())?;
        }
        Ok(())
    }
}

static IDEMPOTENCY_COUNTER: AtomicU64 = AtomicU64::new(1);

fn generate_idempotency_key() -> IdempotencyKey {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = IDEMPOTENCY_COUNTER.fetch_add(1, Ordering::Relaxed);
    IdempotencyKey::new(format!("sdk-{now:016x}-{counter:016x}"))
        .expect("generated idempotency key must satisfy the wire grammar")
}
