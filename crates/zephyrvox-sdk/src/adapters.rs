//! Optional media-adapter traits for device and codec integrations.

use std::{future::Future, pin::Pin};

use thiserror::Error;

use zephyrvox_types::{CodecCapability, InboundMedia, OutboundMedia, StreamTypeId};

/// An owned, sendable future used by media adapter traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors returned by a host-provided media adapter.
#[derive(Debug, Error)]
pub enum MediaError {
    /// The adapter has been stopped and cannot accept another frame.
    #[error("media adapter is closed")]
    Closed,
    /// The adapter cannot handle the requested codec capability.
    #[error("unsupported media codec: {0}")]
    UnsupportedCodec(String),
    /// A PCM or encoded frame violates the adapter's own input contract.
    #[error("invalid media frame: {0}")]
    InvalidFrame(String),
    /// The host adapter failed for an implementation-specific reason.
    #[error("media adapter failed: {0}")]
    Adapter(String),
}

/// One interleaved signed-16 PCM frame supplied to an [`AudioCodec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PcmFrame {
    /// Server-registered stream type for the encoded output.
    pub stream_type: StreamTypeId,
    /// Sampling rate of the interleaved samples.
    pub sample_rate: u32,
    /// Number of interleaved channels.
    pub channels: u8,
    /// Interleaved signed-16 samples.
    pub samples: Vec<i16>,
}

impl PcmFrame {
    /// Creates a PCM frame after validating its basic shape.
    ///
    /// The sample vector may be empty only when a host deliberately uses a
    /// codec-specific drain marker; normal audio frames should contain at
    /// least one sample per channel.
    ///
    /// # Errors
    ///
    /// Returns [`MediaError::InvalidFrame`] when the rate or channel count is
    /// zero, or when the sample count cannot contain complete interleaved
    /// samples.
    pub fn new(
        stream_type: StreamTypeId,
        sample_rate: u32,
        channels: u8,
        samples: Vec<i16>,
    ) -> Result<Self, MediaError> {
        if sample_rate == 0 {
            return Err(MediaError::InvalidFrame(
                "PCM sample rate must be positive".to_owned(),
            ));
        }
        if channels == 0 {
            return Err(MediaError::InvalidFrame(
                "PCM channel count must be positive".to_owned(),
            ));
        }
        if samples.len() % usize::from(channels) != 0 {
            return Err(MediaError::InvalidFrame(
                "PCM samples must be interleaved by complete channels".to_owned(),
            ));
        }
        Ok(Self {
            stream_type,
            sample_rate,
            channels,
            samples,
        })
    }
}

/// Supplies already encoded outbound media frames from a host device or
/// capture pipeline.
pub trait MediaSource: Send {
    /// Produces the next frame, or `None` after the source has reached EOF.
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<OutboundMedia>, MediaError>>;
}

/// Consumes encoded media received from other voice participants.
pub trait MediaSink: Send {
    /// Delivers one frame to the host playback, recording, or forwarding
    /// pipeline.
    fn push_frame(&mut self, frame: InboundMedia) -> BoxFuture<'_, Result<(), MediaError>>;
}

/// Encodes and decodes PCM according to one server-advertised capability.
///
/// The SDK does not provide a device implementation, Opus implementation, or
/// jitter buffer.  An adapter must validate that its own PCM format matches
/// [`Self::capability`] and must preserve the `stream_type` selected by the
/// host when it creates outbound media.
pub trait AudioCodec: Send + Sync {
    /// Returns the immutable server capability this adapter implements.
    fn capability(&self) -> &CodecCapability;

    /// Encodes one PCM frame into an outbound codec payload.
    fn encode(&self, input: PcmFrame) -> BoxFuture<'_, Result<OutboundMedia, MediaError>>;

    /// Decodes one received payload into PCM without changing its speaker or
    /// stream identity outside the returned frame.
    fn decode(&self, input: InboundMedia) -> BoxFuture<'_, Result<PcmFrame, MediaError>>;
}
