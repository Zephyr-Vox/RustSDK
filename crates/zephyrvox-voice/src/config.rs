use std::{
    collections::HashMap,
    fmt,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use zeroize::Zeroizing;

use crate::VoiceError;
use zephyrvox_types::{SessionId, StreamTypeId, VoiceEndpoint, VoiceSessionId};
use zephyrvox_wire::{PROTOCOL_VERSION, max_payload};

/// A 32-byte voice master key with a redacted debug representation.
///
/// The key is supplied by the authenticated HTTP join response for a newly
/// created encrypted session. It is never serialized by this crate and is
/// zeroized when the last owning value is dropped.
pub struct SessionKey(Zeroizing<[u8; 32]>);

impl SessionKey {
    /// Creates a key from exactly 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Creates a key from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Configuration`] when `bytes` is not 32 bytes.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, VoiceError> {
        let value: [u8; 32] = bytes.try_into().map_err(|_| {
            VoiceError::Configuration("voice session key must be exactly 32 bytes".to_owned())
        })?;
        Ok(Self::from_bytes(value))
    }

    /// Returns the key bytes to the internal packet codec.
    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Clone for SessionKey {
    fn clone(&self) -> Self {
        Self::from_bytes(*self.0)
    }
}

impl PartialEq for SessionKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}

impl Eq for SessionKey {}

impl fmt::Debug for SessionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted-voice-session-key>")
    }
}

/// A zeroizing cache for encrypted session keys that are reused by HTTP join.
#[derive(Clone, Default)]
pub struct SessionKeyCache {
    keys: Arc<Mutex<HashMap<SessionId, SessionKey>>>,
}

impl SessionKeyCache {
    /// Creates an empty key cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores or replaces the key for one session identifier.
    pub fn insert(&self, session_id: VoiceSessionId, key: SessionKey) {
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        keys.insert(session_id, key);
    }

    /// Returns a cloned key for a session, if one is cached.
    pub fn get(&self, session_id: VoiceSessionId) -> Option<SessionKey> {
        let keys = self
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        keys.get(&session_id).cloned()
    }

    /// Removes a session key from the cache and returns its owned value.
    ///
    /// The returned key is zeroized when that value is dropped.
    pub fn remove(&self, session_id: VoiceSessionId) -> Option<SessionKey> {
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        keys.remove(&session_id)
    }

    /// Removes and zeroizes every cached key.
    pub fn clear(&self) {
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        keys.clear();
    }

    /// Returns the number of cached session keys.
    pub fn len(&self) -> usize {
        let keys = self
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        keys.len()
    }

    /// Reports whether no session keys are cached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for SessionKeyCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionKeyCache")
            .field("entries", &self.len())
            .finish()
    }
}

/// Validated values obtained from one HTTP voice join response and metadata.
///
/// This type intentionally does not depend on `zephyrvox-http`. The HTTP
/// adapter decodes its base64 key into [`SessionKey`] and passes this value to
/// the UDP transport. A newly created encrypted session supplies the key;
/// reused sessions may omit it when a [`SessionKeyCache`] contains the same
/// session identifier.
pub struct VoiceSessionConfig {
    endpoint: VoiceEndpoint,
    session_id: VoiceSessionId,
    protocol_version: u16,
    encrypted: bool,
    max_payload: usize,
    expires_at: i64,
    session_key: Option<SessionKey>,
    allowed_stream_types: Option<Vec<StreamTypeId>>,
}

impl VoiceSessionConfig {
    /// Creates a v1 configuration for an already-negotiated voice session.
    ///
    /// `session_key` is required for newly created encrypted sessions and may
    /// be omitted for a reused encrypted session whose key is in a
    /// [`SessionKeyCache`]. It must be absent for plaintext sessions.
    /// Plaintext is accepted only because the server's
    /// metadata may explicitly describe a local development deployment; this
    /// constructor never treats TLS failure as a reason to select it.
    ///
    /// # Errors
    ///
    /// Returns an error for wildcard endpoints, invalid expiry or payload
    /// limits, a protocol version mismatch, or an invalid key lifecycle.
    pub fn new(
        endpoint: VoiceEndpoint,
        session_id: VoiceSessionId,
        encrypted: bool,
        max_payload: usize,
        expires_at: i64,
        session_key: Option<SessionKey>,
    ) -> Result<Self, VoiceError> {
        Self::new_with_protocol_version(
            endpoint,
            session_id,
            u16::from(PROTOCOL_VERSION),
            encrypted,
            max_payload,
            expires_at,
            session_key,
        )
    }

    /// Creates a configuration while explicitly checking the shared protocol
    /// version returned by metadata and voice join.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Configuration`] for an invalid endpoint, expiry,
    /// payload limit, or key combination, and
    /// [`VoiceError::ProtocolVersionMismatch`] for an unsupported version.
    pub fn new_with_protocol_version(
        endpoint: VoiceEndpoint,
        session_id: VoiceSessionId,
        protocol_version: u16,
        encrypted: bool,
        max_payload: usize,
        expires_at: i64,
        session_key: Option<SessionKey>,
    ) -> Result<Self, VoiceError> {
        validate_endpoint(&endpoint)?;
        let expected = u16::from(PROTOCOL_VERSION);
        if protocol_version != expected {
            return Err(VoiceError::ProtocolVersionMismatch {
                expected,
                actual: protocol_version,
            });
        }
        if max_payload == 0 || max_payload > max_payload_for(encrypted) {
            return Err(VoiceError::Configuration(format!(
                "voice payload limit must be between 1 and {} bytes",
                max_payload_for(encrypted)
            )));
        }
        if expires_at <= 0 {
            return Err(VoiceError::Configuration(
                "voice session expiry must be positive".to_owned(),
            ));
        }
        if !encrypted && session_key.is_some() {
            return Err(VoiceError::Configuration(
                "plaintext sessions must not carry a session key".to_owned(),
            ));
        }
        Ok(Self {
            endpoint,
            session_id,
            protocol_version,
            encrypted,
            max_payload,
            expires_at,
            session_key,
            allowed_stream_types: None,
        })
    }

    /// Restricts media to the stream types advertised by metadata.
    ///
    /// An empty list is rejected because it would create a session that can
    /// only send protocol heartbeats. Type zero is always rejected here.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Configuration`] when the list is empty or
    /// contains the reserved heartbeat stream.
    pub fn with_stream_types(
        mut self,
        stream_types: impl IntoIterator<Item = StreamTypeId>,
    ) -> Result<Self, VoiceError> {
        let mut values = stream_types.into_iter().collect::<Vec<_>>();
        values.sort_by_key(|stream_type| stream_type.get());
        values.dedup_by_key(|stream_type| stream_type.get());
        if values.is_empty() || values.iter().any(|stream_type| stream_type.is_heartbeat()) {
            return Err(VoiceError::Configuration(
                "voice stream types must contain business types only".to_owned(),
            ));
        }
        self.allowed_stream_types = Some(values);
        Ok(self)
    }

    /// Returns the advertised UDP endpoint.
    pub fn endpoint(&self) -> &VoiceEndpoint {
        &self.endpoint
    }

    /// Returns the negotiated session identifier.
    pub const fn session_id(&self) -> VoiceSessionId {
        self.session_id
    }

    /// Returns the shared protocol version.
    pub const fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    /// Reports whether UDP payloads use AES-GCM.
    pub const fn encrypted(&self) -> bool {
        self.encrypted
    }

    /// Returns the negotiated maximum encoded media payload length.
    pub const fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Returns the join response's advertised expiry in Unix milliseconds.
    pub const fn expires_at(&self) -> i64 {
        self.expires_at
    }

    /// Returns the stream allow-list, if metadata supplied one.
    pub fn allowed_stream_types(&self) -> Option<&[StreamTypeId]> {
        self.allowed_stream_types.as_deref()
    }

    /// Resolves the inline key or the cached key for this session.
    ///
    /// Encrypted configurations without either source fail here instead of
    /// silently downgrading to plaintext.
    pub(crate) fn resolve_key(
        &self,
        cache: Option<&SessionKeyCache>,
    ) -> Result<Option<SessionKey>, VoiceError> {
        if !self.encrypted {
            return Ok(None);
        }
        self.session_key
            .clone()
            .or_else(|| cache.and_then(|cache| cache.get(self.session_id)))
            .map(Some)
            .ok_or(VoiceError::KeyUnavailable)
    }

    /// Stores an inline key in the optional cache before the config is dropped.
    pub(crate) fn cache_inline_key(&self, cache: Option<&SessionKeyCache>) {
        if let (Some(cache), Some(key)) = (cache, self.session_key.clone()) {
            cache.insert(self.session_id, key);
        }
    }
}

impl fmt::Debug for VoiceSessionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VoiceSessionConfig")
            .field("endpoint", &self.endpoint)
            .field("session_id", &self.session_id)
            .field("protocol_version", &self.protocol_version)
            .field("encrypted", &self.encrypted)
            .field("max_payload", &self.max_payload)
            .field("expires_at", &self.expires_at)
            .field(
                "session_key",
                &self.session_key.as_ref().map(|_| "<redacted>"),
            )
            .field("allowed_stream_types", &self.allowed_stream_types)
            .finish()
    }
}

/// Returns the protocol payload ceiling for the negotiated encryption mode.
fn max_payload_for(encrypted: bool) -> usize {
    max_payload(encrypted)
}

/// Rejects endpoints that cannot be used as a concrete remote UDP address.
fn validate_endpoint(endpoint: &VoiceEndpoint) -> Result<(), VoiceError> {
    if endpoint.host.trim().is_empty() || endpoint.port == 0 {
        return Err(VoiceError::Configuration(
            "voice endpoint must have a host and non-zero port".to_owned(),
        ));
    }
    if endpoint
        .host
        .parse::<IpAddr>()
        .is_ok_and(|address| address.is_unspecified())
    {
        return Err(VoiceError::Configuration(
            "voice endpoint must not be a wildcard address".to_owned(),
        ));
    }
    Ok(())
}
