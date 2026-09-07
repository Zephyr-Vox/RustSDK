#![allow(dead_code)]

use zephyrvox_types::{SessionId, StreamTypeId, VoiceEndpoint, VoiceSessionId};
use zephyrvox_voice::{SessionKey, VoiceSessionConfig};

pub fn session_id() -> VoiceSessionId {
    SessionId::parse_hex("00112233445566778899aabbccddeeff").expect("session id")
}

pub fn speaker_id() -> zephyrvox_types::Snowflake {
    zephyrvox_types::Snowflake::new(42).expect("speaker id")
}

pub fn endpoint(port: u16) -> VoiceEndpoint {
    VoiceEndpoint {
        host: "127.0.0.1".to_owned(),
        port,
    }
}

pub fn plain_config(port: u16) -> VoiceSessionConfig {
    VoiceSessionConfig::new(
        endpoint(port),
        session_id(),
        false,
        1158,
        1_800_000_000_000,
        None,
    )
    .expect("plain voice config")
}

pub fn encrypted_config(port: u16) -> VoiceSessionConfig {
    VoiceSessionConfig::new(
        endpoint(port),
        session_id(),
        true,
        1142,
        1_800_000_000_000,
        Some(SessionKey::from_bytes([0x11; 32])),
    )
    .expect("encrypted voice config")
}

pub fn streams(config: VoiceSessionConfig) -> VoiceSessionConfig {
    config
        .with_stream_types([StreamTypeId::new(1), StreamTypeId::new(2)])
        .expect("stream types")
}
