use std::str::FromStr;

use zephyrvox_wire::{
    HeaderError, IdempotencyKey, MAX_IDEMPOTENCY_KEY_LENGTH, MIN_IDEMPOTENCY_KEY_LENGTH,
};

#[test]
fn idempotency_keys_enforce_the_server_grammar() {
    let key = IdempotencyKey::new("a".repeat(MIN_IDEMPOTENCY_KEY_LENGTH)).expect("minimum key");
    assert_eq!(key.as_str().len(), MIN_IDEMPOTENCY_KEY_LENGTH);
    assert_eq!(key.to_string(), key.as_str());
    assert_eq!(
        IdempotencyKey::from_str(&"Z".repeat(MAX_IDEMPOTENCY_KEY_LENGTH))
            .expect("maximum key")
            .as_str()
            .len(),
        MAX_IDEMPOTENCY_KEY_LENGTH
    );
}

#[test]
fn idempotency_keys_reject_invalid_length_and_characters() {
    assert!(matches!(
        IdempotencyKey::new("short"),
        Err(HeaderError::InvalidIdempotencyKeyLength { .. })
    ));
    assert!(matches!(
        IdempotencyKey::new(format!("{}!", "a".repeat(MIN_IDEMPOTENCY_KEY_LENGTH))),
        Err(HeaderError::InvalidIdempotencyKeyCharacter { .. })
    ));
    assert!(matches!(
        IdempotencyKey::new("é".repeat(MIN_IDEMPOTENCY_KEY_LENGTH)),
        Err(HeaderError::InvalidIdempotencyKeyCharacter { .. })
    ));
    assert!(matches!(
        IdempotencyKey::new("a".repeat(MAX_IDEMPOTENCY_KEY_LENGTH + 1)),
        Err(HeaderError::InvalidIdempotencyKeyLength { .. })
    ));
}
