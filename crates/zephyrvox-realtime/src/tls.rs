use std::sync::{Arc, Mutex};

use rustls::{
    CertificateError, DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme,
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        verify_server_cert_signed_by_trust_anchor, verify_server_name,
    },
    crypto::{self, CryptoProvider},
    pki_types::{CertificateDer, ServerName, UnixTime},
    server::ParsedCertificate,
};
use sha2::{Digest, Sha256};

use crate::RealtimeError;
use zephyrvox_wire::Fingerprint;

#[derive(Debug, Clone, Copy)]
pub(crate) enum PinFailure {
    PinMismatch {
        /// Fingerprint carried by the card.
        expected: Fingerprint,
        /// Fingerprint computed from the peer certificate.
        actual: Fingerprint,
    },
    /// The certificate could not be checked.
    InvalidCertificate,
}

type PinFailureState = Arc<Mutex<Option<PinFailure>>>;

/// Builds the custom rustls verifier used by the WebSocket connector.
///
/// Certificate hostname, validity, and signature checks remain enabled. The
/// server-card SPKI pin is checked before the chain is accepted, and any TLS
/// failure terminates the connection attempt without a plaintext fallback.
pub(crate) fn pinned_client_config(
    expected: Fingerprint,
) -> Result<(rustls::ClientConfig, PinFailureState), RealtimeError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let failure = Arc::new(Mutex::new(None));
    let verifier = Arc::new(PinnedServerCertVerifier {
        expected,
        provider: Arc::clone(&provider),
        failure: Arc::clone(&failure),
    });
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|error| RealtimeError::Transport(format!("invalid TLS configuration: {error}")))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok((config, failure))
}

#[derive(Debug)]
struct PinnedServerCertVerifier {
    expected: Fingerprint,
    provider: Arc<CryptoProvider>,
    failure: Arc<Mutex<Option<PinFailure>>>,
}

impl PinnedServerCertVerifier {
    /// Records the first certificate classification for transport diagnostics.
    fn record_failure(&self, failure: PinFailure) {
        if let Ok(mut recorded) = self.failure.lock() {
            if recorded.is_none() {
                *recorded = Some(failure);
            }
        }
    }
}

impl PinFailure {
    /// Converts the verifier classification into the public transport error.
    pub(crate) fn into_error(self) -> RealtimeError {
        match self {
            Self::PinMismatch { expected, actual } => RealtimeError::TlsPinMismatch {
                expected: expected.to_hex(),
                actual: actual.to_hex(),
            },
            Self::InvalidCertificate => RealtimeError::TlsCertificateInvalid,
        }
    }
}

impl ServerCertVerifier for PinnedServerCertVerifier {
    /// Checks the card pin before normal hostname, validity, and signature
    /// verification completes.
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let actual =
            Fingerprint::from_bytes(spki_sha256(end_entity.as_ref()).ok_or_else(|| {
                self.record_failure(PinFailure::InvalidCertificate);
                TlsError::InvalidCertificate(CertificateError::BadEncoding)
            })?);
        if !actual.ct_eq(&self.expected) {
            self.record_failure(PinFailure::PinMismatch {
                expected: self.expected,
                actual,
            });
            return Err(TlsError::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }

        let certificate = ParsedCertificate::try_from(end_entity).inspect_err(|_| {
            self.record_failure(PinFailure::InvalidCertificate);
        })?;
        verify_server_name(&certificate, server_name).inspect_err(|_| {
            self.record_failure(PinFailure::InvalidCertificate);
        })?;
        let mut roots = RootCertStore::empty();
        roots.add(end_entity.clone()).inspect_err(|_| {
            self.record_failure(PinFailure::InvalidCertificate);
        })?;
        verify_server_cert_signed_by_trust_anchor(
            &certificate,
            &roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )
        .inspect_err(|_| {
            self.record_failure(PinFailure::InvalidCertificate);
        })?;
        Ok(ServerCertVerified::assertion())
    }

    /// Delegates TLS 1.2 handshake signature verification to rustls' provider.
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    /// Delegates TLS 1.3 handshake signature verification to rustls' provider.
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    /// Returns the signature schemes supported by the configured provider.
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Extracts the DER SubjectPublicKeyInfo sequence and hashes it with SHA-256.
fn spki_sha256(certificate: &[u8]) -> Option<[u8; 32]> {
    let (tag, certificate_start, certificate_end, certificate_next) = read_tlv(certificate, 0)?;
    if tag != 0x30 || certificate_next != certificate.len() {
        return None;
    }
    let (tag, tbs_start, tbs_end, _) =
        read_tlv(&certificate[certificate_start..certificate_end], 0)?;
    if tag != 0x30 {
        return None;
    }
    let tbs = &certificate[certificate_start + tbs_start..certificate_start + tbs_end];
    let mut offset = 0;
    if read_tlv(tbs, offset)?.0 == 0xa0 {
        offset = read_tlv(tbs, offset)?.3;
    }
    for _ in 0..5 {
        offset = read_tlv(tbs, offset)?.3;
    }
    let (_, _, _, spki_next) = read_tlv(tbs, offset)?;
    Some(Sha256::digest(&tbs[offset..spki_next]).into())
}

/// Reads one definite-length DER TLV without allocating.
fn read_tlv(input: &[u8], offset: usize) -> Option<(u8, usize, usize, usize)> {
    let tag = *input.get(offset)?;
    let length_byte = *input.get(offset + 1)?;
    let (length, content_start) = if length_byte & 0x80 == 0 {
        (length_byte as usize, offset + 2)
    } else {
        let length_bytes = (length_byte & 0x7f) as usize;
        if length_bytes == 0 || length_bytes > 8 {
            return None;
        }
        let mut length = 0_usize;
        for byte in input.get(offset + 2..offset + 2 + length_bytes)? {
            length = length.checked_shl(8)?.checked_add(*byte as usize)?;
        }
        (length, offset + 2 + length_bytes)
    };
    let content_end = content_start.checked_add(length)?;
    if content_end > input.len() {
        return None;
    }
    Some((tag, content_start, content_end, content_end))
}
