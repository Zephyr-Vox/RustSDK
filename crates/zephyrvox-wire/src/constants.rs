/// The one protocol version shared by HTTP, WebSocket, and UDP voice.
pub const PROTOCOL_VERSION: u8 = 1;

/// The four-byte UDP voice magic.
pub const MAGIC: [u8; 4] = *b"ZVX1";

/// Text form of the UDP voice magic.
pub const MAGIC_TEXT: &str = "ZVX1";

/// Outer UDP header size in bytes.
pub const OUTER_HEADER_SIZE: usize = 30;

/// Per-channel UDP header size in bytes.
pub const CHANNEL_HEADER_SIZE: usize = 12;

/// AES-GCM authentication tag size in bytes.
pub const TAG_SIZE: usize = 16;

/// Hard maximum UDP datagram size.
pub const MAX_PACKET_SIZE: usize = 1200;

/// Maximum encrypted media payload after both protocol headers and the tag.
pub const MAX_PAYLOAD_ENCRYPTED: usize =
    MAX_PACKET_SIZE - OUTER_HEADER_SIZE - CHANNEL_HEADER_SIZE - TAG_SIZE;

/// Maximum plaintext media payload when encryption is disabled by the server.
pub const MAX_PAYLOAD_PLAINTEXT: usize = MAX_PACKET_SIZE - OUTER_HEADER_SIZE - CHANNEL_HEADER_SIZE;

/// Reserved stream type for voice heartbeats.
pub const HEARTBEAT_STREAM_TYPE: u8 = 0;

/// Returns the payload limit for the selected packet protection mode.
pub const fn max_payload(encrypted: bool) -> usize {
    if encrypted {
        MAX_PAYLOAD_ENCRYPTED
    } else {
        MAX_PAYLOAD_PLAINTEXT
    }
}

/// Reports whether a stream type is available for application media.
pub const fn is_business_stream_type(stream_type: u8) -> bool {
    stream_type != HEARTBEAT_STREAM_TYPE
}
