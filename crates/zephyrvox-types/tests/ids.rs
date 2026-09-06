use std::str::FromStr;

use serde_json::json;
use zephyrvox_types::{ControlConnectionId, Cursor, Geid, SessionId, Snowflake, StreamEpoch};

#[test]
fn snowflakes_use_lossless_decimal_strings_and_accept_legacy_numbers() {
    let snowflake = Snowflake::new(9_007_199_254_740_993).expect("valid Snowflake");
    assert_eq!(
        serde_json::to_value(snowflake).expect("serialize"),
        json!("9007199254740993")
    );
    assert_eq!(
        serde_json::from_str::<Snowflake>(r#""9007199254740993""#).expect("string Snowflake"),
        snowflake
    );
    assert_eq!(
        serde_json::from_str::<Snowflake>("123").expect("numeric Snowflake"),
        Snowflake::new(123).expect("valid Snowflake")
    );
}

#[test]
fn snowflake_range_is_strict() {
    assert!(matches!(
        Snowflake::new(0),
        Err(zephyrvox_types::IdError::ZeroSnowflake)
    ));
    assert!(matches!(
        Snowflake::new(i64::MAX as u64 + 1),
        Err(zephyrvox_types::IdError::SnowflakeOutOfRange(_))
    ));
    assert!(serde_json::from_str::<Snowflake>("1.5").is_err());
}

#[test]
fn fixed_ids_round_trip_as_lowercase_hex() {
    let bytes = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc,
        0xfe,
    ];
    let session = SessionId::from_bytes(bytes).expect("non-zero session");
    let control = ControlConnectionId::from_bytes(bytes).expect("non-zero control id");
    let epoch = StreamEpoch::from_bytes(bytes).expect("non-zero epoch");
    let expected = "0123456789abcdef1032547698badcfe";
    assert_eq!(session.to_hex(), expected);
    assert_eq!(control.to_string(), expected);
    assert_eq!(epoch.to_string(), expected);
    assert_eq!(
        serde_json::from_str::<SessionId>(&format!(r#""{expected}""#)).expect("decode session"),
        session
    );
    assert!(SessionId::from_str(&expected.to_ascii_uppercase()).is_err());
    assert!(SessionId::from_bytes([0; 16]).is_err());
}

#[test]
fn cursors_are_opaque_but_not_empty() {
    let cursor = Cursor::new("opaque-cursor").expect("non-empty cursor");
    assert_eq!(cursor.as_str(), "opaque-cursor");
    assert_eq!(
        serde_json::to_string(&cursor).expect("serialize cursor"),
        r#""opaque-cursor""#
    );
    assert!(Cursor::new("").is_err());
    assert!(serde_json::from_str::<Cursor>(r#""""#).is_err());
}

#[test]
fn geids_allow_zero_checkpoints_but_reject_negative_values() {
    assert_eq!(
        serde_json::from_str::<Geid>("0").expect("zero GEID"),
        Geid::new(0)
    );
    assert!(serde_json::from_str::<Geid>("-1").is_err());
    assert_eq!(Geid::new(42).to_string(), "42");
}
