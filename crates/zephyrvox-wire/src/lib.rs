//! Wire-level contracts shared by the SDK transports.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod constants;
mod envelope;
mod headers;
mod server_card;

pub use constants::{
    CHANNEL_HEADER_SIZE, HEARTBEAT_STREAM_TYPE, MAGIC, MAGIC_TEXT, MAX_PACKET_SIZE,
    MAX_PAYLOAD_ENCRYPTED, MAX_PAYLOAD_PLAINTEXT, OUTER_HEADER_SIZE, PROTOCOL_VERSION, TAG_SIZE,
    is_business_stream_type, max_payload,
};
pub use envelope::{ApiEnvelope, ApiError, EnvelopeError};
pub use headers::{
    AUTHORIZATION, COMMAND_ID, CONTROL_CONNECTION, ETAG, GEID, HeaderError, IDEMPOTENCY_KEY,
    IF_MATCH, IdempotencyKey, MAX_IDEMPOTENCY_KEY_LENGTH, MIN_IDEMPOTENCY_KEY_LENGTH, PARENT_ETAG,
    PARENT_IF_MATCH, STATE_CURSOR, STREAM_EPOCH, SYNC_REQUIRED,
};
pub use server_card::{Fingerprint, ServerCard, ServerCardError, TransportScheme};
