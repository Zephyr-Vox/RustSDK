mod support;

use zephyrvox_types::{StreamTypeId, VoiceEndpoint};
use zephyrvox_voice::{PacketCodec, SessionKey, SessionKeyCache, VoiceError, VoiceSessionConfig};
use zephyrvox_wire::PROTOCOL_VERSION;

#[test]
fn encrypted_reuse_resolves_only_from_the_explicit_key_cache() {
    let session_id = support::session_id();
    let config = VoiceSessionConfig::new(
        support::endpoint(40_100),
        session_id,
        true,
        1_142,
        1_800_000_000_000,
        None,
    )
    .expect("reused encrypted config");
    let cache = SessionKeyCache::new();
    cache.insert(session_id, SessionKey::from_bytes([7; 32]));

    let codec = PacketCodec::from_config(&config, Some(&cache)).expect("cached key");
    assert!(codec.encrypted());
    assert_eq!(cache.len(), 1);

    let missing = PacketCodec::from_config(&config, None);
    assert!(matches!(missing, Err(VoiceError::KeyUnavailable)));
}

#[test]
fn key_cache_is_cloneable_and_debug_does_not_expose_key_material() {
    let cache = SessionKeyCache::new();
    let session_id = support::session_id();
    cache.insert(session_id, SessionKey::from_bytes([0xAB; 32]));

    let cloned = cache.clone();
    assert_eq!(
        cloned.get(session_id),
        Some(SessionKey::from_bytes([0xAB; 32]))
    );
    assert!(format!("{cache:?}").contains("entries: 1"));
    assert!(!format!("{cache:?}").contains("AB"));
    assert_eq!(
        cache.remove(session_id),
        Some(SessionKey::from_bytes([0xAB; 32]))
    );
    assert!(cache.is_empty());
}

#[test]
fn configuration_rejects_protocol_and_endpoint_contract_violations() {
    let key = SessionKey::from_bytes([1; 32]);
    let plaintext_with_key = VoiceSessionConfig::new(
        support::endpoint(40_100),
        support::session_id(),
        false,
        1_158,
        1_800_000_000_000,
        Some(key.clone()),
    );
    assert!(matches!(
        plaintext_with_key,
        Err(VoiceError::Configuration(message)) if message.contains("must not carry")
    ));

    let wrong_version = VoiceSessionConfig::new_with_protocol_version(
        support::endpoint(40_100),
        support::session_id(),
        u16::from(PROTOCOL_VERSION) + 1,
        false,
        1_158,
        1_800_000_000_000,
        None,
    );
    assert!(matches!(
        wrong_version,
        Err(VoiceError::ProtocolVersionMismatch { .. })
    ));

    let wildcard = VoiceSessionConfig::new(
        VoiceEndpoint {
            host: "0.0.0.0".to_owned(),
            port: 40_100,
        },
        support::session_id(),
        false,
        1_158,
        1_800_000_000_000,
        None,
    );
    assert!(matches!(wildcard, Err(VoiceError::Configuration(_))));

    let too_large = VoiceSessionConfig::new(
        support::endpoint(40_100),
        support::session_id(),
        true,
        1_143,
        1_800_000_000_000,
        Some(key),
    );
    assert!(matches!(too_large, Err(VoiceError::Configuration(_))));
}

#[test]
fn stream_allow_list_excludes_reserved_heartbeat_type() {
    let config = VoiceSessionConfig::new(
        support::endpoint(40_100),
        support::session_id(),
        false,
        1_158,
        1_800_000_000_000,
        None,
    )
    .expect("config");

    assert!(matches!(
        config.with_stream_types([]),
        Err(VoiceError::Configuration(_))
    ));
    let config = VoiceSessionConfig::new(
        support::endpoint(40_100),
        support::session_id(),
        false,
        1_158,
        1_800_000_000_000,
        None,
    )
    .expect("config");
    assert!(matches!(
        config.with_stream_types([StreamTypeId::new(0)]),
        Err(VoiceError::Configuration(_))
    ));
}
