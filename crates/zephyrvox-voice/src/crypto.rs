use aes_gcm::{
    Aes256Gcm,
    aead::{Aead, KeyInit, Payload},
};
use hkdf::Hkdf;
use sha2::Sha256;

use crate::{PacketError, SessionKey};
use zephyrvox_types::VoiceSessionId;

const C2S_INFO: &[u8] = b"zephyrvox-voice-c2s-v1";
const S2C_INFO: &[u8] = b"zephyrvox-voice-s2c-v1";

/// The independent AES-GCM ciphers for the two UDP directions.
pub(crate) struct DirectionKeys {
    pub(crate) c2s: Aes256Gcm,
    pub(crate) s2c: Aes256Gcm,
}

/// Derives the direction-separated AES-256-GCM ciphers from one join key.
pub(crate) fn derive_direction_keys(
    session_id: VoiceSessionId,
    master_key: &SessionKey,
) -> DirectionKeys {
    let hkdf = Hkdf::<Sha256>::new(Some(session_id.as_bytes()), master_key.as_bytes());
    let c2s = derive_cipher(&hkdf, C2S_INFO);
    let s2c = derive_cipher(&hkdf, S2C_INFO);
    DirectionKeys { c2s, s2c }
}

/// Computes the deterministic 96-bit AES-GCM nonce for one transport sequence.
pub fn nonce_for(sequence: u64) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[4..].copy_from_slice(&sequence.to_be_bytes());
    nonce
}

/// Expands one direction-specific AES-256 key and clears temporary material.
fn derive_cipher(hkdf: &Hkdf<Sha256>, info: &[u8]) -> Aes256Gcm {
    let mut material = [0_u8; 32];
    hkdf.expand(info, &mut material)
        .expect("HKDF output length is fixed at 32 bytes");
    let cipher = Aes256Gcm::new_from_slice(&material)
        .expect("AES-256 accepts the fixed 32-byte HKDF output");
    zeroize::Zeroize::zeroize(&mut material);
    cipher
}

/// Authenticates and encrypts one packet body with its outer header as AAD.
pub(crate) fn seal(
    cipher: &Aes256Gcm,
    sequence: u64,
    header: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, PacketError> {
    if sequence == 0 {
        return Err(PacketError::SequenceZero);
    }
    let nonce = nonce_for(sequence);
    cipher
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: plaintext,
                aad: header,
            },
        )
        .map_err(|_| PacketError::AuthenticationFailed)
}

/// Authenticates and decrypts one packet body with its outer header as AAD.
pub(crate) fn open(
    cipher: &Aes256Gcm,
    sequence: u64,
    header: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, PacketError> {
    if sequence == 0 {
        return Err(PacketError::SequenceZero);
    }
    let nonce = nonce_for(sequence);
    cipher
        .decrypt(
            (&nonce).into(),
            Payload {
                msg: ciphertext,
                aad: header,
            },
        )
        .map_err(|_| PacketError::AuthenticationFailed)
}
