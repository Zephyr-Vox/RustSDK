use zephyrvox_wire::{
    CHANNEL_HEADER_SIZE, HEARTBEAT_STREAM_TYPE, MAGIC, MAGIC_TEXT, MAX_PACKET_SIZE,
    MAX_PAYLOAD_ENCRYPTED, MAX_PAYLOAD_PLAINTEXT, OUTER_HEADER_SIZE, PROTOCOL_VERSION, TAG_SIZE,
    is_business_stream_type, max_payload,
};

#[test]
fn udp_wire_limits_match_the_server_contract() {
    assert_eq!(MAGIC, *b"ZVX1");
    assert_eq!(MAGIC_TEXT, "ZVX1");
    assert_eq!(PROTOCOL_VERSION, 1);
    assert_eq!(OUTER_HEADER_SIZE, 30);
    assert_eq!(CHANNEL_HEADER_SIZE, 12);
    assert_eq!(TAG_SIZE, 16);
    assert_eq!(MAX_PACKET_SIZE, 1200);
    assert_eq!(MAX_PAYLOAD_ENCRYPTED, 1142);
    assert_eq!(MAX_PAYLOAD_PLAINTEXT, 1158);
    assert_eq!(max_payload(true), MAX_PAYLOAD_ENCRYPTED);
    assert_eq!(max_payload(false), MAX_PAYLOAD_PLAINTEXT);
    assert!(!is_business_stream_type(HEARTBEAT_STREAM_TYPE));
    assert!(is_business_stream_type(1));
}
