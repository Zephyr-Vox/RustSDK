use std::{collections::HashSet, net::IpAddr};

use crate::{
    ApiClient, HttpError, HttpMethod, RequestOptions, client::AuthRequirement,
    response::ApiResponse,
};
use zephyrvox_types::Metadata;
use zephyrvox_types::StateSnapshot;
use zephyrvox_wire::PROTOCOL_VERSION;

impl ApiClient {
    /// Discovers protocol, UDP, codec, and stream capabilities.
    ///
    /// This request is unauthenticated.  The returned protocol version and
    /// capability values are validated before they are exposed; the card's
    /// HTTP/TLS decision is never changed by metadata.
    pub async fn metadata(&self) -> Result<Metadata, HttpError> {
        Ok(self.metadata_response().await?.data)
    }

    /// Discovers protocol capabilities and preserves response headers.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::ProtocolVersionMismatch`] for a server version this
    /// SDK cannot use, or [`HttpError::Wire`] for an invalid endpoint,
    /// capability list, or stream registry.
    pub async fn metadata_response(&self) -> Result<ApiResponse<Metadata>, HttpError> {
        let request = self.empty_request(HttpMethod::Get, "metadata", RequestOptions::new())?;
        let response: ApiResponse<Metadata> =
            self.send_json(request, AuthRequirement::None).await?;
        if response.data.protocol_version != u16::from(PROTOCOL_VERSION) {
            return Err(HttpError::ProtocolVersionMismatch {
                expected: u16::from(PROTOCOL_VERSION),
                actual: response.data.protocol_version,
            });
        }
        validate_metadata(&response.data)?;
        Ok(response)
    }

    /// Fetches the authenticated replace-style client state snapshot.
    ///
    /// The snapshot is a complete state replacement for a future realtime
    /// synchronizer; this method does not install it into another client-side
    /// state object.
    pub async fn snapshot(&self) -> Result<StateSnapshot, HttpError> {
        Ok(self.snapshot_response().await?.data)
    }

    /// Fetches the authenticated state snapshot and preserves response headers.
    ///
    /// The request requires an access token and retries one `401` after the
    /// shared refresh singleflight succeeds.
    pub async fn snapshot_response(&self) -> Result<ApiResponse<StateSnapshot>, HttpError> {
        let request =
            self.empty_request(HttpMethod::Get, "state/snapshot", RequestOptions::new())?;
        self.send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}

/// Validates metadata before an application uses its advertised voice endpoint.
fn validate_metadata(metadata: &Metadata) -> Result<(), HttpError> {
    if !valid_metadata_host(&metadata.voice_endpoint.host) || metadata.voice_endpoint.port == 0 {
        return Err(HttpError::Wire(
            "metadata contains an invalid voice endpoint".to_owned(),
        ));
    }

    if metadata.codecs.is_empty() {
        return Err(HttpError::Wire(
            "metadata must advertise at least one codec".to_owned(),
        ));
    }
    if metadata
        .features
        .iter()
        .any(|feature| !valid_metadata_text(feature))
    {
        return Err(HttpError::Wire(
            "metadata contains an invalid feature name".to_owned(),
        ));
    }
    let mut codec_names = HashSet::new();
    for codec in &metadata.codecs {
        if !valid_metadata_text(&codec.name)
            || codec.clock_rate == 0
            || codec.channels == 0
            || codec.ptime.is_empty()
            || codec.ptime.contains(&0)
            || !codec_names.insert(codec.name.to_ascii_lowercase())
        {
            return Err(HttpError::Wire(
                "metadata contains an invalid or duplicate codec".to_owned(),
            ));
        }
    }

    if metadata.voice_stream_types.is_empty() {
        return Err(HttpError::Wire(
            "metadata must advertise at least one voice stream type".to_owned(),
        ));
    }
    let mut stream_ids = HashSet::new();
    for stream in &metadata.voice_stream_types {
        if stream.id.is_heartbeat()
            || !valid_metadata_text(&stream.name)
            || !valid_metadata_text(&stream.mute_kind)
            || !stream_ids.insert(stream.id.get())
        {
            return Err(HttpError::Wire(
                "metadata contains an invalid or duplicate voice stream type".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Accepts capability labels without permitting ambiguous whitespace/control
/// characters at the protocol boundary.
fn valid_metadata_text(value: &str) -> bool {
    !value.is_empty() && value == value.trim() && value.bytes().all(|byte| !byte.is_ascii_control())
}

/// Applies the server-side endpoint host grammar without depending on config.
fn valid_metadata_host(host: &str) -> bool {
    let trimmed = host.trim();
    if trimmed != host || trimmed.is_empty() || trimmed.len() > 253 {
        return false;
    }
    let host = trimmed;
    if let Ok(address) = host.parse::<IpAddr>() {
        return !address.is_unspecified();
    }
    if host.starts_with('.')
        || host.ends_with('.')
        || host.contains("..")
        || host.starts_with('[')
        || host.ends_with(']')
    {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}
