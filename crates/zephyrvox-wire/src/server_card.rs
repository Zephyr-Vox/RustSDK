use std::{fmt, net::IpAddr, str::FromStr};

use thiserror::Error;
use url::{Url, form_urlencoded::Serializer};

use crate::PROTOCOL_VERSION;

/// The two server-card transport schemes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportScheme {
    /// Plain HTTP and WebSocket transport.
    Plain,
    /// TLS-protected HTTP and WebSocket transport.
    Tls,
}

impl TransportScheme {
    /// Returns the canonical server-card scheme text.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "zephyrvox",
            Self::Tls => "zephyrvoxs",
        }
    }

    /// Reports whether the transport requires TLS pinning.
    pub const fn is_tls(self) -> bool {
        matches!(self, Self::Tls)
    }
}

/// A 32-byte SPKI SHA-256 fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    /// Creates a fingerprint from raw SHA-256 bytes.
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Returns the raw fingerprint bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parses exactly 64 hexadecimal characters.
    pub fn parse_hex(value: &str) -> Result<Self, ServerCardError> {
        if value.len() != 64 {
            return Err(ServerCardError::InvalidFingerprint {
                value: value.to_owned(),
            });
        }

        let mut output = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_value(pair[0]).ok_or_else(|| ServerCardError::InvalidFingerprint {
                value: value.to_owned(),
            })?;
            let low = hex_value(pair[1]).ok_or_else(|| ServerCardError::InvalidFingerprint {
                value: value.to_owned(),
            })?;
            output[index] = (high << 4) | low;
        }
        Ok(Self(output))
    }

    /// Returns canonical lowercase hexadecimal text.
    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            output.push(hex_digit(byte >> 4));
            output.push(hex_digit(byte & 0x0f));
        }
        output
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn hex_digit(value: u8) -> char {
    b"0123456789abcdef"[value as usize] as char
}

/// A parsed and validated CommunityServer connection card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerCard {
    scheme: TransportScheme,
    host: String,
    port: u16,
    fingerprint: Option<Fingerprint>,
    invite: Option<String>,
    protocol_version: u8,
}

impl ServerCard {
    /// Constructs a v1 card and validates its TLS and host invariants.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        scheme: TransportScheme,
        fingerprint: Option<Fingerprint>,
        invite: Option<String>,
    ) -> Result<Self, ServerCardError> {
        let host = normalize_host(host.into())?;
        if port == 0 {
            return Err(ServerCardError::InvalidPort);
        }
        if scheme.is_tls() && fingerprint.is_none() {
            return Err(ServerCardError::MissingFingerprint);
        }
        if !scheme.is_tls() && fingerprint.is_some() {
            return Err(ServerCardError::UnexpectedFingerprint);
        }

        let invite = invite.and_then(|invite| {
            let invite = invite.trim().to_owned();
            (!invite.is_empty()).then_some(invite)
        });

        Ok(Self {
            scheme,
            host,
            port,
            fingerprint,
            invite,
            protocol_version: PROTOCOL_VERSION,
        })
    }

    /// Parses a canonical or compatible server-card URL.
    pub fn parse(value: &str) -> Result<Self, ServerCardError> {
        let url =
            Url::parse(value).map_err(|error| ServerCardError::InvalidUrl(error.to_string()))?;
        let scheme = match url.scheme() {
            "zephyrvox" => TransportScheme::Plain,
            "zephyrvoxs" => TransportScheme::Tls,
            other => return Err(ServerCardError::UnsupportedScheme(other.to_owned())),
        };
        if !url.username().is_empty() || url.password().is_some() {
            return Err(ServerCardError::InvalidUrl(
                "server cards must not contain user information".to_owned(),
            ));
        }
        if url.fragment().is_some() {
            return Err(ServerCardError::InvalidUrl(
                "server cards must not contain a fragment".to_owned(),
            ));
        }
        if !url.path().is_empty() && url.path() != "/" {
            return Err(ServerCardError::InvalidUrl(
                "server cards must not contain a path".to_owned(),
            ));
        }

        let port = url.port().ok_or(ServerCardError::MissingPort)?;
        let host = url
            .host_str()
            .ok_or_else(|| ServerCardError::InvalidHost(String::new()))?;

        let mut version = None;
        let mut fingerprint = None;
        let mut invite = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "v" => {
                    if version.is_some() {
                        return Err(ServerCardError::DuplicateQuery("v".to_owned()));
                    }
                    version = Some(
                        value
                            .parse::<u8>()
                            .map_err(|_| ServerCardError::InvalidVersion(value.into_owned()))?,
                    );
                }
                "fp" => {
                    if fingerprint.is_some() {
                        return Err(ServerCardError::DuplicateQuery("fp".to_owned()));
                    }
                    fingerprint = Some(Fingerprint::parse_hex(&value)?);
                }
                "invite" => {
                    if invite.is_some() {
                        return Err(ServerCardError::DuplicateQuery("invite".to_owned()));
                    }
                    invite = Some(value.into_owned());
                }
                other => return Err(ServerCardError::UnknownQuery(other.to_owned())),
            }
        }

        let version = version.ok_or(ServerCardError::MissingVersion)?;
        if version != PROTOCOL_VERSION {
            return Err(ServerCardError::UnsupportedVersion(version));
        }
        Self::new(host, port, scheme, fingerprint, invite)
    }

    /// Returns the transport scheme.
    pub const fn scheme(&self) -> TransportScheme {
        self.scheme
    }

    /// Returns the normalized host without IPv6 brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the explicit server-card port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the pinned SPKI fingerprint, if this is a TLS card.
    pub const fn fingerprint(&self) -> Option<Fingerprint> {
        self.fingerprint
    }

    /// Returns the optional invite token.
    pub fn invite(&self) -> Option<&str> {
        self.invite.as_deref()
    }

    /// Returns the one protocol version carried by this card.
    pub const fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Renders the card as a stable URL with deterministic query ordering.
    pub fn render(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let mut query = Serializer::new(String::new());
        query.append_pair("v", &self.protocol_version.to_string());
        if let Some(fingerprint) = self.fingerprint {
            query.append_pair("fp", &fingerprint.to_hex());
        }
        if let Some(invite) = &self.invite {
            query.append_pair("invite", invite);
        }
        format!(
            "{}://{}:{}?{}",
            self.scheme.as_str(),
            host,
            self.port,
            query.finish()
        )
    }

    /// Converts the rendered card into a URL for HTTP/WebSocket base paths.
    pub fn to_url(&self) -> Result<Url, ServerCardError> {
        Url::parse(&self.render()).map_err(|error| ServerCardError::InvalidUrl(error.to_string()))
    }
}

fn normalize_host(host: String) -> Result<String, ServerCardError> {
    let host = host.trim();
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() || host.len() > 253 || host.chars().any(char::is_whitespace) {
        return Err(ServerCardError::InvalidHost(host.to_owned()));
    }

    if let Ok(address) = IpAddr::from_str(host) {
        return Ok(address.to_string());
    }
    if host.contains(':') || host.contains('/') || host.contains('\\') {
        return Err(ServerCardError::InvalidHost(host.to_owned()));
    }
    for label in host.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || label
                .bytes()
                .any(|value| !(value.is_ascii_alphanumeric() || value == b'-'))
        {
            return Err(ServerCardError::InvalidHost(host.to_owned()));
        }
    }
    Ok(host.to_ascii_lowercase())
}

/// Errors raised while parsing or rendering a server card.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServerCardError {
    /// The URL parser rejected the card.
    #[error("invalid server card URL: {0}")]
    InvalidUrl(String),
    /// The scheme was not one of the two supported card schemes.
    #[error("unsupported server card scheme: {0}")]
    UnsupportedScheme(String),
    /// The URL did not carry an explicit port.
    #[error("server card must contain an explicit port")]
    MissingPort,
    /// The host was empty or outside the accepted DNS/IP grammar.
    #[error("invalid server card host: {0}")]
    InvalidHost(String),
    /// The card omitted the protocol version query.
    #[error("server card must contain a v query parameter")]
    MissingVersion,
    /// The card carried a malformed protocol version.
    #[error("invalid protocol version: {0}")]
    InvalidVersion(String),
    /// The card used a version not supported by this SDK.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    /// A query parameter appeared more than once.
    #[error("duplicate server card query parameter: {0}")]
    DuplicateQuery(String),
    /// A query parameter is not part of the card contract.
    #[error("unknown server card query parameter: {0}")]
    UnknownQuery(String),
    /// TLS cards must pin an SPKI fingerprint.
    #[error("TLS server cards require an SPKI fingerprint")]
    MissingFingerprint,
    /// Plain cards must not carry TLS-only pinning.
    #[error("plain server cards must not carry an SPKI fingerprint")]
    UnexpectedFingerprint,
    /// The fingerprint was not exactly 64 hexadecimal characters.
    #[error("invalid SPKI fingerprint: {value}")]
    InvalidFingerprint { value: String },
    /// The port was zero.
    #[error("server card port must be non-zero")]
    InvalidPort,
}
