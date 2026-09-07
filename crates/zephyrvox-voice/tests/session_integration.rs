mod support;

use std::time::Duration;

use tokio::{net::UdpSocket, time::timeout};
use zephyrvox_types::StreamTypeId;
use zephyrvox_voice::{
    PacketCodec, PacketControl, PacketError, RevocationReason, SessionKeyCache, VoiceError,
    VoiceSession, VoiceStatus,
};

#[tokio::test]
async fn session_sends_heartbeat_media_and_receives_relayed_frames() {
    let server = UdpSocket::bind("127.0.0.1:0").await.expect("server socket");
    let server_address = server.local_addr().expect("server address");
    let config = support::streams(support::plain_config(server_address.port()));
    let client_socket = UdpSocket::bind("127.0.0.1:0").await.expect("client socket");
    client_socket
        .connect(server_address)
        .await
        .expect("client connect");
    let client_address = client_socket.local_addr().expect("client address");
    let codec = PacketCodec::from_config(&config, None).expect("server codec");
    let session = VoiceSession::from_connected_socket(config, client_socket)
        .await
        .expect("voice session");
    let mut datagram = [0_u8; 1200];
    let length = timeout(Duration::from_secs(1), server.recv(&mut datagram))
        .await
        .expect("heartbeat timeout")
        .expect("heartbeat read");
    let heartbeat = codec.decode_client(&datagram[..length]).expect("heartbeat");
    assert_eq!(heartbeat.transport_seq, 1);
    assert_eq!(heartbeat.control, Some(PacketControl::Heartbeat));

    assert!(matches!(
        session
            .send(StreamTypeId::new(1), vec![0_u8; session.max_payload() + 1])
            .await,
        Err(VoiceError::Packet(PacketError::PayloadTooLarge { .. }))
    ));
    assert!(matches!(
        session.send(StreamTypeId::new(3), b"unknown").await,
        Err(VoiceError::Packet(PacketError::UnknownStreamType(3)))
    ));

    session
        .send(StreamTypeId::new(1), b"microphone")
        .await
        .expect("send media");
    let length = timeout(Duration::from_secs(1), server.recv(&mut datagram))
        .await
        .expect("media timeout")
        .expect("media read");
    let outbound = codec.decode_client(&datagram[..length]).expect("media");
    assert_eq!(outbound.transport_seq, 2);
    assert_eq!(outbound.channel_seq, 0);
    assert_eq!(outbound.payload, b"microphone".as_slice());

    let inbound = codec
        .encode_server_media(
            1,
            19,
            StreamTypeId::new(2),
            support::speaker_id(),
            b"desktop",
        )
        .expect("relay packet");
    server
        .send_to(&inbound, client_address)
        .await
        .expect("relay write");
    let frame = timeout(Duration::from_secs(1), session.recv())
        .await
        .expect("media receive timeout")
        .expect("media receive");
    assert_eq!(frame.speaker_id, support::speaker_id());
    assert_eq!(frame.stream_type, StreamTypeId::new(2));
    assert_eq!(frame.channel_seq, 19);
    assert_eq!(frame.transport_seq, 1);
    assert_eq!(frame.payload, b"desktop".as_slice());

    session.close().await.expect("close session");
    assert_eq!(session.status(), VoiceStatus::Disconnected);
    assert!(matches!(
        timeout(Duration::from_secs(1), session.recv())
            .await
            .expect("closed media receiver timeout"),
        Err(VoiceError::Closed)
    ));
}

#[tokio::test]
async fn encrypted_session_drops_duplicates_and_stops_on_revocation() {
    let server = UdpSocket::bind("127.0.0.1:0").await.expect("server socket");
    let server_address = server.local_addr().expect("server address");
    let config = support::streams(support::encrypted_config(server_address.port()));
    let key_cache = SessionKeyCache::new();
    let client_socket = UdpSocket::bind("127.0.0.1:0").await.expect("client socket");
    client_socket
        .connect(server_address)
        .await
        .expect("client connect");
    let client_address = client_socket.local_addr().expect("client address");
    let codec = PacketCodec::from_config(&config, Some(&key_cache)).expect("server codec");
    let session = VoiceSession::from_connected_socket_with_key_cache(
        config,
        client_socket,
        key_cache.clone(),
    )
    .await
    .expect("voice session");
    let mut media = session.subscribe_media();
    let mut datagram = [0_u8; 1200];
    let _ = timeout(Duration::from_secs(1), server.recv(&mut datagram))
        .await
        .expect("heartbeat timeout")
        .expect("heartbeat read");

    let inbound = codec
        .encode_server_media(1, 3, StreamTypeId::new(1), support::speaker_id(), b"opus")
        .expect("encrypted relay packet");
    server
        .send_to(&inbound, client_address)
        .await
        .expect("write");
    server
        .send_to(&inbound, client_address)
        .await
        .expect("duplicate");
    let frame = timeout(Duration::from_secs(1), media.recv())
        .await
        .expect("frame timeout")
        .expect("frame");
    assert_eq!(frame.payload, b"opus".as_slice());
    assert!(
        timeout(Duration::from_millis(50), media.recv())
            .await
            .is_err()
    );

    let revoke = codec
        .encode_revocation(2, RevocationReason::Revoked)
        .expect("revocation");
    server
        .send_to(&revoke, client_address)
        .await
        .expect("revoke");
    let mut status = session.subscribe_status();
    timeout(Duration::from_secs(1), async {
        while !matches!(
            *status.borrow(),
            VoiceStatus::Revoked(RevocationReason::Revoked)
        ) {
            status.changed().await.expect("status channel");
        }
    })
    .await
    .expect("revocation status timeout");
    assert!(key_cache.is_empty());
    assert!(matches!(
        session.send(StreamTypeId::new(1), b"late").await,
        Err(VoiceError::Revoked(RevocationReason::Revoked))
    ));
}
