mod support;

use bytes::Bytes;
use zephyrvox_types::StreamTypeId;
use zephyrvox_voice::{
    PacketCodec, PacketControl, PacketError, RevocationReason, VoiceSessionConfig,
};
use zephyrvox_wire::{CHANNEL_HEADER_SIZE, MAGIC, MAX_PACKET_SIZE, OUTER_HEADER_SIZE};

#[test]
fn plaintext_packet_layout_uses_big_endian_fields() {
    let config = support::streams(support::plain_config(40_100));
    let codec = PacketCodec::from_config(&config, None).expect("codec");
    let packet = codec
        .encode_media(7, 0x4242, StreamTypeId::new(2), b"opus")
        .expect("packet");

    assert_eq!(packet.len(), OUTER_HEADER_SIZE + CHANNEL_HEADER_SIZE + 4);
    assert_eq!(&packet[..4], &MAGIC);
    assert_eq!(packet[4], 1);
    assert_eq!(packet[5], 0);
    assert_eq!(&packet[6..22], support::session_id().as_bytes());
    assert_eq!(&packet[22..30], &7_u64.to_be_bytes());
    assert_eq!(packet[30], 2);
    assert_eq!(&packet[31..33], &0x4242_u16.to_be_bytes());
    assert_eq!(packet[33], 0);
    assert_eq!(&packet[34..42], &0_u64.to_be_bytes());
    assert_eq!(&packet[42..], b"opus");

    let decoded = codec.decode_client(&packet).expect("decoded packet");
    assert_eq!(decoded.transport_seq, 7);
    assert_eq!(decoded.channel_seq, 0x4242);
    assert_eq!(decoded.stream_type, StreamTypeId::new(2));
    assert_eq!(decoded.speaker_id, None);
    assert_eq!(decoded.payload, Bytes::from_static(b"opus"));
    assert_eq!(decoded.control, None);
}

#[test]
fn encrypted_packet_round_trips_and_keeps_direction_keys_separate() {
    let config = support::streams(support::encrypted_config(40_100));
    let codec = PacketCodec::from_config(&config, None).expect("codec");
    let outbound = codec
        .encode_media(9, 3, StreamTypeId::new(1), b"encrypted opus")
        .expect("outbound packet");
    let decoded = codec.decode_client(&outbound).expect("client decode");
    assert_eq!(decoded.payload, Bytes::from_static(b"encrypted opus"));
    assert!(matches!(
        codec.decode_server(&outbound),
        Err(PacketError::AuthenticationFailed)
    ));

    let inbound = codec
        .encode_server_media(4, 17, StreamTypeId::new(2), support::speaker_id(), b"relay")
        .expect("inbound packet");
    let decoded = codec.decode_server(&inbound).expect("server decode");
    assert_eq!(decoded.transport_seq, 4);
    assert_eq!(decoded.speaker_id, Some(support::speaker_id()));
    assert_eq!(decoded.payload, Bytes::from_static(b"relay"));
}

#[test]
fn heartbeat_revocation_and_payload_limits_are_enforced() {
    let plain = VoiceSessionConfig::new(
        support::endpoint(40_100),
        support::session_id(),
        false,
        100,
        1_800_000_000_000,
        None,
    )
    .expect("config");
    let codec = PacketCodec::from_config(&plain, None).expect("codec");
    let heartbeat = codec.encode_heartbeat(1).expect("heartbeat");
    let decoded = codec.decode_client(&heartbeat).expect("heartbeat decode");
    assert_eq!(decoded.control, Some(PacketControl::Heartbeat));
    assert_eq!(decoded.payload, Bytes::new());

    let revoke = codec
        .encode_revocation(2, RevocationReason::Revoked)
        .expect("revocation");
    let decoded = codec.decode_server(&revoke).expect("revocation decode");
    assert_eq!(
        decoded.control,
        Some(PacketControl::Revocation(RevocationReason::Revoked))
    );
    assert_eq!(
        codec.decode_client(&revoke),
        Err(PacketError::InvalidControlPacket)
    );

    assert!(matches!(
        codec.encode_media(3, 0, StreamTypeId::new(1), &[0_u8; 101]),
        Err(PacketError::PayloadTooLarge {
            actual: 101,
            maximum: 100
        })
    ));
    assert!(matches!(
        codec.encode_media(4, 0, StreamTypeId::new(0), b"bad"),
        Err(PacketError::HeartbeatStream)
    ));
    assert_eq!(MAX_PACKET_SIZE, 1200);
}

#[test]
fn malformed_and_tampered_packets_are_rejected() {
    let config = support::streams(support::encrypted_config(40_100));
    let codec = PacketCodec::from_config(&config, None).expect("codec");
    let packet = codec
        .encode_media(1, 0, StreamTypeId::new(1), b"audio")
        .expect("packet");

    assert_eq!(
        codec.decode_server(&packet[..packet.len() - 1]),
        Err(PacketError::AuthenticationFailed)
    );
    let mut tampered = packet.clone();
    tampered[22] ^= 1;
    assert!(matches!(
        codec.decode_client(&tampered),
        Err(PacketError::AuthenticationFailed)
    ));
    let mut bad_magic = packet;
    bad_magic[0] = b'X';
    assert_eq!(
        codec.decode_client(&bad_magic),
        Err(PacketError::InvalidMagic)
    );
}
