use std::sync::{Arc, RwLock as StdRwLock, Weak};

use tokio::sync::{RwLock, broadcast, watch};

use crate::StateApplyError;
use zephyrvox_types::{ClientState, StateEvent, StateSnapshot, VoiceAuthority};

mod projection;

use projection::{apply_known_event, validate_event};

/// The result of applying an ordered event to a [`StateStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The event was accepted and published to both state and event
    /// subscribers.
    Applied,
}

/// An atomic, clonable client-side state projection.
///
/// The store uses one async write lock to clone and update the projection,
/// then publishes the resulting `Arc<ClientState>` through a Tokio watch
/// channel. Subscribers therefore observe either the old complete state or
/// the new complete state; no field-level intermediate state is exposed.
/// Ordered events are copied to a bounded broadcast channel after the same
/// commit. A slow event subscriber receives `RecvError::Lagged`, while a slow
/// state watcher simply observes the latest snapshot.
#[derive(Clone)]
pub struct StateStore {
    state: Arc<RwLock<Arc<ClientState>>>,
    state_tx: watch::Sender<Arc<ClientState>>,
    event_tx: Arc<StdRwLock<broadcast::Sender<StateEvent>>>,
    event_generation: watch::Sender<u64>,
}

/// Receives ordered state events while respecting snapshot replacement
/// boundaries.
///
/// A receiver automatically discards events buffered before a snapshot
/// replacement and resumes from the replacement's fresh event channel. It
/// otherwise preserves Tokio broadcast semantics, including `Lagged` and
/// `Closed` errors.
pub struct StateEventReceiver {
    event_tx: Weak<StdRwLock<broadcast::Sender<StateEvent>>>,
    receiver: broadcast::Receiver<StateEvent>,
    generation: watch::Receiver<u64>,
    observed_generation: u64,
}

impl StateEventReceiver {
    /// Creates a receiver bound to the store's current ordered-event channel.
    fn new(store: &StateStore) -> Self {
        // Hold the sender read lock while taking the generation snapshot. A
        // replacement holds the matching write lock through both operations,
        // so a new subscriber can never pair an old channel with a new
        // generation marker.
        let event_tx = store
            .event_tx
            .read()
            .expect("state event channel lock poisoned");
        let receiver = event_tx.subscribe();
        let generation = store.event_generation.subscribe();
        let observed_generation = *generation.borrow();
        Self {
            event_tx: Arc::downgrade(&store.event_tx),
            receiver,
            generation,
            observed_generation,
        }
    }

    /// Receives the next event after the latest complete snapshot boundary.
    ///
    /// Events that were queued before a snapshot replacement are discarded
    /// before this method returns. A slow receiver still receives Tokio's
    /// usual `Lagged` error when it falls behind the current event channel.
    pub async fn recv(&mut self) -> Result<StateEvent, broadcast::error::RecvError> {
        loop {
            if !self.refresh_generation() {
                return Err(broadcast::error::RecvError::Closed);
            }
            let observed_generation = self.observed_generation;
            let received = tokio::select! {
                result = self.receiver.recv() => Some(result),
                changed = self.generation.changed() => {
                    if changed.is_err() {
                        return Err(broadcast::error::RecvError::Closed);
                    }
                    if !self.refresh_generation() {
                        return Err(broadcast::error::RecvError::Closed);
                    }
                    None
                }
            };
            let Some(result) = received else {
                continue;
            };
            if self.current_generation() != observed_generation {
                if !self.refresh_generation() {
                    return Err(broadcast::error::RecvError::Closed);
                }
                continue;
            }
            return result;
        }
    }

    /// Tries to receive the next event without waiting.
    ///
    /// The return type and lag behavior match
    /// [`tokio::sync::broadcast::Receiver::try_recv`].
    pub fn try_recv(&mut self) -> Result<StateEvent, broadcast::error::TryRecvError> {
        loop {
            if !self.refresh_generation() {
                return Err(broadcast::error::TryRecvError::Closed);
            }
            let observed_generation = self.observed_generation;
            match self.receiver.try_recv() {
                Ok(_event) if self.current_generation() != observed_generation => {
                    if !self.refresh_generation() {
                        return Err(broadcast::error::TryRecvError::Closed);
                    }
                }
                result => return result,
            }
        }
    }

    /// Creates a fresh receiver at the current snapshot boundary.
    pub fn resubscribe(&self) -> Self {
        let (receiver, observed_generation) = self
            .event_tx
            .upgrade()
            .map(|event_tx| {
                // As in `new`, pair the channel subscription and generation
                // marker while the replacement writer is excluded.
                let event_tx = event_tx.read().expect("state event channel lock poisoned");
                let receiver = event_tx.subscribe();
                let observed_generation = self.current_generation();
                (receiver, observed_generation)
            })
            .unwrap_or_else(|| (self.receiver.resubscribe(), self.current_generation()));
        Self {
            event_tx: Weak::clone(&self.event_tx),
            receiver,
            generation: self.generation.clone(),
            observed_generation,
        }
    }

    /// Returns the latest snapshot-generation marker observed by this receiver.
    fn current_generation(&self) -> u64 {
        *self.generation.borrow()
    }

    /// Replaces the underlying broadcast receiver after a snapshot boundary.
    fn refresh_generation(&mut self) -> bool {
        let generation = self.current_generation();
        if generation == self.observed_generation {
            return true;
        }
        let Some(event_tx) = self.event_tx.upgrade() else {
            return false;
        };
        let sender = event_tx
            .read()
            .expect("state event channel lock poisoned")
            .clone();
        self.receiver = sender.subscribe();
        self.observed_generation = generation;
        true
    }
}

impl StateStore {
    /// Creates a store from one authoritative HTTP snapshot.
    pub fn from_snapshot(snapshot: StateSnapshot) -> Self {
        let state = Arc::new(ClientState::from_snapshot(snapshot));
        let (state_tx, _) = watch::channel(Arc::clone(&state));
        let (event_tx, _) = broadcast::channel(256);
        let (event_generation, _) = watch::channel(0_u64);
        Self {
            state: Arc::new(RwLock::new(state)),
            state_tx,
            event_tx: Arc::new(StdRwLock::new(event_tx)),
            event_generation,
        }
    }

    /// Returns a clone of the latest complete state without retaining the
    /// store's read lock.
    pub async fn snapshot(&self) -> Arc<ClientState> {
        Arc::clone(&*self.state.read().await)
    }

    /// Subscribes to latest-only complete state replacements.
    pub fn subscribe_state(&self) -> watch::Receiver<Arc<ClientState>> {
        self.state_tx.subscribe()
    }

    /// Subscribes to ordered state events.
    pub fn subscribe_events(&self) -> StateEventReceiver {
        StateEventReceiver::new(self)
    }

    /// Replaces the entire local projection at a snapshot boundary.
    ///
    /// Pending event subscribers are not sent synthetic events. Instead, the
    /// ordered event receiver switches to a fresh channel so events from the
    /// previous state boundary cannot be delivered after the replacement.
    ///
    /// Returns the previous voice authority when the replacement invalidates
    /// it. The connection coordinator turns that result into a host-visible
    /// VoiceLost notification.
    pub async fn replace_snapshot(&self, snapshot: StateSnapshot) -> Option<VoiceAuthority> {
        let replacement = Arc::new(ClientState::from_snapshot(snapshot));
        let mut current = self.state.write().await;
        let voice_lost = match (
            &current.self_user.voice_authority,
            &replacement.self_user.voice_authority,
        ) {
            (Some(previous), Some(next)) if previous == next => None,
            (Some(previous), _) => Some(previous.clone()),
            _ => None,
        };
        let next_generation = self.event_generation.borrow().wrapping_add(1);
        let (event_tx, _) = broadcast::channel(256);
        // Keep the write lock until the generation marker and state are
        // published. New subscribers take the same lock while pairing their
        // channel with the marker, making the snapshot boundary linearizable.
        let mut event_sender = self
            .event_tx
            .write()
            .expect("state event channel lock poisoned");
        *event_sender = event_tx;
        self.event_generation.send_replace(next_generation);
        *current = Arc::clone(&replacement);
        self.state_tx.send_replace(replacement);
        voice_lost
    }

    /// Replaces only the authenticated cursor after `sync.complete`.
    ///
    /// The server may complete a replay with no visible events, so the cursor
    /// can advance while the state payload remains byte-for-byte unchanged.
    /// The operation is still published as a new complete `ClientState`.
    pub async fn update_cursor(&self, cursor: zephyrvox_types::Cursor) {
        let mut current = self.state.write().await;
        let mut next = (**current).clone();
        next.cursor = cursor;
        let next = Arc::new(next);
        *current = Arc::clone(&next);
        self.state_tx.send_replace(next);
    }

    /// Validates, applies, and publishes one state event atomically.
    ///
    /// The event is broadcast only after the complete replacement has been
    /// installed. If validation or payload decoding fails, neither the state
    /// nor the subscriber channels are changed; the caller should fetch a new
    /// snapshot for an unknown state evolution.
    ///
    /// # Errors
    ///
    /// Returns [`StateApplyError`] for invalid ordering, unknown event types,
    /// or malformed replacement payloads.
    pub async fn apply_event(&self, event: StateEvent) -> Result<ApplyOutcome, StateApplyError> {
        let mut current = self.state.write().await;
        validate_event(&event, &current)?;
        let mut next = (**current).clone();
        apply_known_event(&mut next, &event)?;
        next.cursor = event.cursor.clone();
        next.geid = event.geid;
        let next = Arc::new(next);
        *current = Arc::clone(&next);
        self.state_tx.send_replace(next);
        let _ = self
            .event_tx
            .read()
            .expect("state event channel lock poisoned")
            .send(event);
        Ok(ApplyOutcome::Applied)
    }
}
