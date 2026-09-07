//! Tokio-first UDP voice transport for already-negotiated CommunityServer
//! sessions.
//!
//! The crate owns only the media-plane lifecycle. HTTP creates and revokes a
//! business voice session; callers pass the validated join result through
//! [`VoiceSessionConfig`]. The transport never creates a server session,
//! performs codec work, or silently rejoins after a control-plane teardown.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod codec;
mod config;
mod crypto;
mod decode;
mod error;
mod lifecycle;
mod replay;
mod session;
mod worker;

pub use codec::PacketCodec;
pub use config::{SessionKey, SessionKeyCache, VoiceSessionConfig};
pub use crypto::nonce_for;
pub use decode::{DecodedPacket, PacketControl, RevocationReason};
pub use error::{PacketError, VoiceError};
pub use lifecycle::VoiceStatus;
pub use session::VoiceSession;

pub use zephyrvox_types::{InboundMedia, MediaFrame, OutboundMedia, StreamTypeId};
