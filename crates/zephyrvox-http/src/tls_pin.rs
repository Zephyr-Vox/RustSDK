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

use crate::errors::HttpError;
use zephyrvox_wire::Fingerprint;

/// The failure observed while rustls was validating one pinned connection.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TlsVerificationFailure {
    /// The peer certificate had a different SPKI fingerprint.
    PinMismatch {
        /// Fingerprint supplied by the server card.
        expected: Fingerprint,
        /// Fingerprint computed from the peer leaf certificate.
        actual: Fingerprint,
    },
    /// Certificate encoding, validity, chain, or server-name validation failed.
    InvalidCertificate,
}

/// Builds a rustls client config that trusts only the pinned CommunityServer.
pub(crate) fn pinned_client_config(
    expected: Fingerprint,
) -> Result<(rustls::ClientConfig, TlsFailureState), HttpError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let failure = Arc::new(Mutex::new(None));
    let verifier = Arc::new(PinnedServerCertVerifier {
        expected,
        provider: Arc::clone(&provider),
        failure: Arc::clone(&failure),
    });
    rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|error| HttpError::Transport(format!("invalid TLS configuration: {error}")))
        .map(|builder| {
            (
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(verifier)
                    .with_no_client_auth(),
                failure,
            )
        })
}

/// Verifies the pinned certificate while retaining rustls hostname and validity checks.
#[derive(Debug)]
struct PinnedServerCertVerifier {
    expected: Fingerprint,
    provider: Arc<CryptoProvider>,
    failure: Arc<Mutex<Option<TlsVerificationFailure>>>,
}

type TlsFailureState = Arc<Mutex<Option<TlsVerificationFailure>>>;

impl PinnedServerCertVerifier {
    /// Records a validation failure for transport-level error classification.
    fn record_failure(&self, failure: TlsVerificationFailure) {
        if let Ok(mut recorded) = self.failure.lock() {
            *recorded = Some(failure);
        }
    }
}

impl ServerCertVerifier for PinnedServerCertVerifier {
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
                self.record_failure(TlsVerificationFailure::InvalidCertificate);
                TlsError::InvalidCertificate(CertificateError::BadEncoding)
            })?);
        if !actual.ct_eq(&self.expected) {
            self.record_failure(TlsVerificationFailure::PinMismatch {
                expected: self.expected,
                actual,
            });
            return Err(TlsError::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }

        let certificate = ParsedCertificate::try_from(end_entity).inspect_err(|_| {
            self.record_failure(TlsVerificationFailure::InvalidCertificate);
        })?;
        verify_server_name(&certificate, server_name).inspect_err(|_| {
            self.record_failure(TlsVerificationFailure::InvalidCertificate);
        })?;
        let mut roots = RootCertStore::empty();
        roots.add(end_entity.clone()).inspect_err(|_| {
            self.record_failure(TlsVerificationFailure::InvalidCertificate);
        })?;
        verify_server_cert_signed_by_trust_anchor(
            &certificate,
            &roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )
        .inspect_err(|_| {
            self.record_failure(TlsVerificationFailure::InvalidCertificate);
        })?;
        Ok(ServerCertVerified::assertion())
    }

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

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

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
    let (_, _spki_start, _spki_end, spki_next) = read_tlv(tbs, offset)?;
    let digest = Sha256::digest(&tbs[offset..spki_next]);
    Some(digest.into())
}

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
