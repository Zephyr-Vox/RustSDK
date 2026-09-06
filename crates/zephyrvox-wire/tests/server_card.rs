use zephyrvox_wire::{Fingerprint, PROTOCOL_VERSION, ServerCard, ServerCardError, TransportScheme};

#[test]
fn plain_card_round_trips_with_stable_query_order() {
    let card = ServerCard::new(
        "Example.TEST",
        8080,
        TransportScheme::Plain,
        None,
        Some(" invite token ".to_owned()),
    )
    .expect("plain card");
    assert_eq!(
        card.render(),
        "zephyrvox://example.test:8080?v=1&invite=invite+token"
    );
    let parsed = ServerCard::parse(&card.render()).expect("parse card");
    assert_eq!(parsed, card);
    assert_eq!(parsed.protocol_version(), PROTOCOL_VERSION);
}

#[test]
fn tls_card_requires_a_lowercase_fingerprint() {
    let fingerprint =
        Fingerprint::parse_hex("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
            .expect("fingerprint");
    let card = ServerCard::new("::1", 443, TransportScheme::Tls, Some(fingerprint), None)
        .expect("TLS card");
    assert_eq!(
        card.render(),
        "zephyrvoxs://[::1]:443?v=1&fp=aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
    );
    assert_eq!(
        ServerCard::parse(&card.render()).expect("parse TLS card"),
        card
    );
    assert_eq!(card.fingerprint(), Some(fingerprint));
    assert!(fingerprint.ct_eq(&fingerprint));
    assert!(!fingerprint.ct_eq(&Fingerprint::from_bytes([0; 32])));
    assert!(
        Fingerprint::parse_hex("AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899")
            .is_err()
    );
}

#[test]
fn cards_reject_security_and_compatibility_ambiguities() {
    assert!(matches!(
        ServerCard::new("example.test", 443, TransportScheme::Tls, None, None),
        Err(ServerCardError::MissingFingerprint)
    ));
    assert!(matches!(
        ServerCard::new(
            "example.test",
            80,
            TransportScheme::Plain,
            Some(Fingerprint::from_bytes([1; 32])),
            None
        ),
        Err(ServerCardError::UnexpectedFingerprint)
    ));
    assert!(ServerCard::parse("http://example.test:80?v=1").is_err());
    assert!(ServerCard::parse("zephyrvox://example.test:80?v=1&v=1").is_err());
    assert!(ServerCard::parse("zephyrvox://example.test:80?v=2").is_err());
    assert!(ServerCard::parse("zephyrvox://example.test:80?v=1&unknown=x").is_err());
}
