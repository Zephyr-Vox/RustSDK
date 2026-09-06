use zephyrvox_types::{ClientState, ControlConnectionId, Cursor, Geid, StateEvent, StreamEpoch};

use crate::{SyncError, SyncReplay};

const MAX_QUEUED_LIVE_EVENTS: usize = 256;
const MAX_QUEUED_LIVE_BYTES: usize = 1 << 20;

/// The synchronization phase used by the WebSocket protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPhase {
    /// No socket is currently attached.
    Disconnected,
    /// A socket upgrade is in progress.
    Connecting,
    /// `connection.ready` was received.
    Ready,
    /// `sync.hello` was sent and replay is being consumed.
    Syncing,
    /// `sync.complete` was received and live state is accepted.
    Live,
    /// The prior socket ended and a new one is being attempted.
    Reconnecting,
}

/// A pure, Tokio-independent validator for the realtime state handoff.
///
/// `SyncMachine` owns no socket and never mutates [`ClientState`]. It records
/// the cursor/GEID boundary that the state store has installed and validates
/// each frame before the connection coordinator applies it. Keeping this
/// state machine independent makes replay, cursor-gap, and reconnect tests
/// deterministic without a network runtime.
#[derive(Debug, Clone)]
pub struct SyncMachine {
    phase: SyncPhase,
    cursor: Cursor,
    stream_epoch: StreamEpoch,
    geid: Geid,
    control_connection_id: Option<ControlConnectionId>,
    queued_live: Vec<StateEvent>,
    queued_live_bytes: usize,
}

impl SyncMachine {
    /// Creates a machine bound to the current atomic client snapshot.
    pub fn new(state: &ClientState) -> Self {
        Self {
            phase: SyncPhase::Disconnected,
            cursor: state.cursor.clone(),
            stream_epoch: state.stream_epoch,
            geid: state.geid,
            control_connection_id: None,
            queued_live: Vec::new(),
            queued_live_bytes: 0,
        }
    }

    /// Rebinds the machine to a replacement snapshot and discards all events
    /// that belonged to the previous stream boundary.
    pub fn replace_snapshot(&mut self, state: &ClientState) {
        self.cursor = state.cursor.clone();
        self.stream_epoch = state.stream_epoch;
        self.geid = state.geid;
        self.queued_live.clear();
        self.queued_live_bytes = 0;
        self.phase = SyncPhase::Ready;
    }

    /// Returns the current synchronization phase.
    pub const fn phase(&self) -> SyncPhase {
        self.phase
    }

    /// Returns the cursor that must be sent in the next `sync.hello`.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    /// Returns the highest GEID accepted by this machine.
    pub const fn geid(&self) -> Geid {
        self.geid
    }

    /// Returns the stream epoch bound to the current HTTP snapshot.
    ///
    /// WebSocket state events intentionally carry an opaque authenticated
    /// cursor rather than duplicating epoch fields. The coordinator therefore
    /// treats a new HTTP snapshot's epoch as the authoritative replacement
    /// boundary and uses this accessor for diagnostics and host assertions.
    pub const fn stream_epoch(&self) -> StreamEpoch {
        self.stream_epoch
    }

    /// Verifies that an independently supplied checkpoint still belongs to
    /// this snapshot's process epoch.
    ///
    /// This is useful when a host combines an HTTP mutation response with a
    /// realtime handoff. It never decodes the opaque cursor.
    pub fn check_stream_epoch(&self, actual: StreamEpoch) -> Result<(), SyncError> {
        if self.stream_epoch == actual {
            Ok(())
        } else {
            Err(SyncError::StreamEpochChanged {
                expected: self.stream_epoch,
                actual,
            })
        }
    }

    /// Returns the server-assigned control connection ID, if ready.
    pub const fn control_connection_id(&self) -> Option<ControlConnectionId> {
        self.control_connection_id
    }

    /// Records that a socket attempt has started.
    pub fn begin_connecting(&mut self) -> Result<(), SyncError> {
        match self.phase {
            SyncPhase::Disconnected | SyncPhase::Reconnecting => {
                self.phase = SyncPhase::Connecting;
                Ok(())
            }
            phase => Err(SyncError::InvalidPhase(format!(
                "cannot connect from {phase:?}"
            ))),
        }
    }

    /// Accepts the first server frame and transitions to `Ready`.
    pub fn accept_ready(&mut self, id: ControlConnectionId) -> Result<(), SyncError> {
        if !matches!(self.phase, SyncPhase::Connecting | SyncPhase::Reconnecting) {
            return Err(SyncError::InvalidPhase(format!(
                "connection.ready received in {:?}",
                self.phase
            )));
        }
        self.control_connection_id = Some(id);
        self.phase = SyncPhase::Ready;
        Ok(())
    }

    /// Marks the point at which `sync.hello` has been sent.
    pub fn begin_sync(&mut self) -> Result<Cursor, SyncError> {
        if self.phase != SyncPhase::Ready {
            return Err(SyncError::InvalidPhase(format!(
                "sync.hello sent in {:?}",
                self.phase
            )));
        }
        self.phase = SyncPhase::Syncing;
        self.queued_live.clear();
        self.queued_live_bytes = 0;
        Ok(self.cursor.clone())
    }

    /// Validates and accepts one replay batch.
    ///
    /// GEIDs are required to be contiguous and strictly increasing. A gap
    /// means the snapshot/replay boundary is no longer authoritative.
    pub fn accept_replay(&mut self, replay: &SyncReplay) -> Result<(), SyncError> {
        if self.phase != SyncPhase::Syncing {
            return Err(SyncError::InvalidPhase(format!(
                "sync.replay received in {:?}",
                self.phase
            )));
        }
        if replay.events.is_empty() {
            if replay.from_geid != replay.to_geid || replay.from_geid != self.geid {
                return Err(SyncError::ReplayBounds);
            }
            return Ok(());
        }
        let first = replay.events.first().map(|event| event.geid);
        let last = replay.events.last().map(|event| event.geid);
        if first != Some(replay.from_geid) || last != Some(replay.to_geid) {
            return Err(SyncError::ReplayBounds);
        }
        for event in &replay.events {
            self.accept_ordered_event(event)?;
        }
        Ok(())
    }

    /// Validates one live `state.event`; events arriving before completion are
    /// queued until [`Self::accept_complete`] releases the replay boundary.
    pub fn accept_live_event(
        &mut self,
        event: StateEvent,
    ) -> Result<Option<StateEvent>, SyncError> {
        if !matches!(self.phase, SyncPhase::Syncing | SyncPhase::Live) {
            return Err(SyncError::InvalidPhase(format!(
                "state.event received in {:?}",
                self.phase
            )));
        }
        let event_bytes = if self.phase == SyncPhase::Syncing {
            serde_json::to_vec(&event)
                .map(|encoded| encoded.len())
                .map_err(|_| SyncError::QueueLimit)?
        } else {
            0
        };
        if self.phase == SyncPhase::Syncing
            && (self.queued_live.len() >= MAX_QUEUED_LIVE_EVENTS
                || self.queued_live_bytes.saturating_add(event_bytes) > MAX_QUEUED_LIVE_BYTES)
        {
            return Err(SyncError::QueueLimit);
        }
        self.accept_ordered_event(&event)?;
        if self.phase == SyncPhase::Syncing {
            self.queued_live_bytes += event_bytes;
            self.queued_live.push(event);
            Ok(None)
        } else {
            Ok(Some(event))
        }
    }

    /// Accepts `sync.complete`, updates the cursor, and releases queued live
    /// events in their original wire order.
    pub fn accept_complete(
        &mut self,
        complete: &crate::SyncComplete,
    ) -> Result<Vec<StateEvent>, SyncError> {
        if self.phase != SyncPhase::Syncing {
            return Err(SyncError::InvalidPhase(format!(
                "sync.complete received in {:?}",
                self.phase
            )));
        }
        self.cursor = complete.cursor.clone();
        self.phase = SyncPhase::Live;
        self.queued_live_bytes = 0;
        Ok(std::mem::take(&mut self.queued_live))
    }

    /// Resets the machine after `sync.required` without changing the state
    /// store. The caller must replace the store first, then call this method.
    pub fn require_snapshot(&mut self, reason: impl Into<String>) -> Result<(), SyncError> {
        if !matches!(
            self.phase,
            SyncPhase::Ready | SyncPhase::Syncing | SyncPhase::Live
        ) {
            return Err(SyncError::InvalidPhase(format!(
                "sync.required received in {:?}: {}",
                self.phase,
                reason.into()
            )));
        }
        self.queued_live.clear();
        self.queued_live_bytes = 0;
        self.phase = SyncPhase::Ready;
        Ok(())
    }

    /// Marks the socket disconnected while retaining the last durable cursor.
    pub fn disconnect(&mut self) {
        self.queued_live.clear();
        self.queued_live_bytes = 0;
        self.control_connection_id = None;
        self.phase = SyncPhase::Disconnected;
    }

    /// Marks the socket as entering reconnect backoff.
    pub fn reconnecting(&mut self) {
        self.queued_live.clear();
        self.queued_live_bytes = 0;
        self.control_connection_id = None;
        self.phase = SyncPhase::Reconnecting;
    }

    /// Advances the stream boundary only after enforcing contiguous ordering.
    fn accept_ordered_event(&mut self, event: &StateEvent) -> Result<(), SyncError> {
        if event.geid <= self.geid {
            return Err(SyncError::GeidOrder {
                current: self.geid,
                actual: event.geid,
            });
        }
        let expected = Geid::new(self.geid.get().saturating_add(1));
        if event.geid != expected {
            return Err(SyncError::GeidGap {
                expected,
                actual: event.geid,
            });
        }
        self.geid = event.geid;
        self.cursor = event.cursor.clone();
        Ok(())
    }
}
