use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::{RwLock, broadcast, watch};

use crate::StateApplyError;
use zephyrvox_types::{
    Channel, ClientState, Group, Role, SelfUserState, StateEvent, StateSnapshot, UserPresence,
    VoiceAuthority, VoiceMembership,
};

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
    event_tx: broadcast::Sender<StateEvent>,
}

impl StateStore {
    /// Creates a store from one authoritative HTTP snapshot.
    pub fn from_snapshot(snapshot: StateSnapshot) -> Self {
        let state = Arc::new(ClientState::from_snapshot(snapshot));
        let (state_tx, _) = watch::channel(Arc::clone(&state));
        let (event_tx, _) = broadcast::channel(256);
        Self {
            state: Arc::new(RwLock::new(state)),
            state_tx,
            event_tx,
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
    pub fn subscribe_events(&self) -> broadcast::Receiver<StateEvent> {
        self.event_tx.subscribe()
    }

    /// Replaces the entire local projection at a snapshot boundary.
    ///
    /// Pending event subscribers are intentionally not sent synthetic events:
    /// a snapshot is a new atomic boundary, and consumers that need a visible
    /// transition should observe the state watch channel. Existing queued
    /// ordered events are cleared by the connection's synchronization machine.
    pub async fn replace_snapshot(&self, snapshot: StateSnapshot) {
        let replacement = Arc::new(ClientState::from_snapshot(snapshot));
        {
            let mut current = self.state.write().await;
            *current = Arc::clone(&replacement);
        }
        self.state_tx.send_replace(replacement);
    }

    /// Replaces only the authenticated cursor after `sync.complete`.
    ///
    /// The server may complete a replay with no visible events, so the cursor
    /// can advance while the state payload remains byte-for-byte unchanged.
    /// The operation is still published as a new complete `ClientState`.
    pub async fn update_cursor(&self, cursor: zephyrvox_types::Cursor) {
        let replacement = {
            let mut current = self.state.write().await;
            let mut next = (**current).clone();
            next.cursor = cursor;
            let next = Arc::new(next);
            *current = Arc::clone(&next);
            next
        };
        self.state_tx.send_replace(replacement);
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
        let replacement = {
            let mut current = self.state.write().await;
            validate_event(&event, &current)?;
            let mut next = (**current).clone();
            apply_known_event(&mut next, &event)?;
            next.cursor = event.cursor.clone();
            next.geid = event.geid;
            let next = Arc::new(next);
            *current = Arc::clone(&next);
            next
        };
        self.state_tx.send_replace(replacement);
        let _ = self.event_tx.send(event);
        Ok(ApplyOutcome::Applied)
    }
}

/// Checks envelope, cursor, scope, GEID ordering, and the closed event table
/// before any projection field is cloned or changed.
fn validate_event(event: &StateEvent, current: &ClientState) -> Result<(), StateApplyError> {
    if event.frame_type != "state.event" || event.class != "state" {
        return Err(StateApplyError::InvalidEnvelope(
            "state event must have type state.event and class state".to_owned(),
        ));
    }
    if event.cursor.as_str().is_empty() {
        return Err(StateApplyError::InvalidCursor);
    }
    if event.geid <= current.geid {
        return Err(StateApplyError::GeidOrder {
            current: current.geid,
            actual: event.geid,
        });
    }
    if !is_known_state_event(&event.event_type) {
        return Err(StateApplyError::UnknownEvent(event.event_type.clone()));
    }
    if event.scope.kind != "server" && event.scope.kind != "group" && event.scope.kind != "channel"
    {
        return Err(StateApplyError::InvalidEnvelope(format!(
            "unknown event scope {}",
            event.scope.kind
        )));
    }
    if event.scope.kind != "server" && event.scope.id.is_none() {
        return Err(StateApplyError::InvalidEnvelope(
            "non-server event scope has no id".to_owned(),
        ));
    }
    Ok(())
}

/// Applies the replace/upsert/delete semantics for one known event type.
fn apply_known_event(state: &mut ClientState, event: &StateEvent) -> Result<(), StateApplyError> {
    match event.event_type.as_str() {
        "group.created" | "group.updated" => {
            let group = payload::<Group>(event)?;
            upsert_by_id(&mut state.groups, group, |item| item.id);
        }
        "group.deleted" => {
            let deleted = payload::<EntityTombstone>(event)?;
            state.groups.retain(|group| group.id != deleted.entity_id);
            state
                .channels
                .retain(|channel| channel.group_id != Some(deleted.entity_id));
        }
        "channel.created" | "channel.updated" => {
            let channel = payload::<Channel>(event)?;
            upsert_by_id(&mut state.channels, channel, |item| item.id);
        }
        "channel.deleted" => {
            let deleted = payload::<EntityTombstone>(event)?;
            state
                .channels
                .retain(|channel| channel.id != deleted.entity_id);
            state
                .voice_memberships
                .retain(|membership| membership.channel_id != deleted.entity_id);
            if state
                .self_user
                .voice_authority
                .as_ref()
                .is_some_and(|authority| authority.channel_id == deleted.entity_id)
            {
                state.self_user.voice_authority = None;
            }
        }
        "channel.member.joined" => {
            let joined = payload::<MembershipEnvelope>(event)?;
            upsert_membership(&mut state.voice_memberships, joined.membership);
        }
        "channel.member.left" => {
            let left = payload::<MembershipTombstone>(event)?;
            state.voice_memberships.retain(|membership| {
                membership.user_id != left.user_id || membership.channel_id != left.channel_id
            });
        }
        "user.created" | "user.updated" | "presence.updated" => {
            let user = payload::<UserEnvelope>(event)?;
            upsert_user(&mut state.users, user.user);
        }
        "user.deleted" => {
            let deleted = payload::<UserTombstone>(event)?;
            state.users.retain(|user| user.user_id != deleted.user_id);
        }
        "self.updated" => {
            let self_state = payload::<SelfEnvelope>(event)?;
            state.self_user = self_state.self_state;
        }
        "rbac.role.created" | "rbac.role.updated" => {
            let role = payload::<Role>(event)?;
            upsert_by_key(&mut state.roles, role, |item| item.key.clone());
        }
        "rbac.role.deleted" => {
            let deleted = payload::<RoleTombstone>(event)?;
            state.roles.retain(|role| role.key != deleted.role_key);
        }
        "voice.authority.updated" => {
            let authority = payload::<AuthorityEnvelope>(event)?;
            state.self_user.voice_authority = authority.authority;
        }
        "voice.revoked" | "voice.disconnected" | "voice.membership.updated" => {}
        "group.access.updated"
        | "channel.access.updated"
        | "rbac.binding.updated"
        | "rbac.config.updated"
        | "owner.transferred"
        | "moderation.mute.updated"
        | "moderation.mute.removed"
        | "server.updated"
        | "visibility.grant.begin"
        | "visibility.fragment"
        | "visibility.granted"
        | "visibility.revoked"
        | "visibility.tombstone"
        | "visibility.transition.complete" => {}
        event_type => return Err(StateApplyError::UnknownEvent(event_type.to_owned())),
    }
    Ok(())
}

/// Decodes an event-specific payload while preserving its event type in the
/// resulting diagnostic.
fn payload<T: for<'de> Deserialize<'de>>(event: &StateEvent) -> Result<T, StateApplyError> {
    serde_json::from_value(event.data.clone()).map_err(|error| StateApplyError::InvalidPayload {
        event_type: event.event_type.clone(),
        message: error.to_string(),
    })
}

/// Replaces an entity with the same extracted identifier or appends it.
fn upsert_by_id<T, I, F>(items: &mut Vec<T>, item: T, key: F)
where
    I: PartialEq,
    F: Fn(&T) -> I,
{
    let item_key = key(&item);
    if let Some(existing) = items.iter_mut().find(|existing| key(existing) == item_key) {
        *existing = item;
    } else {
        items.push(item);
    }
}

/// Replaces an entity with the same extracted string-like key or appends it.
fn upsert_by_key<T, K, F>(items: &mut Vec<T>, item: T, key: F)
where
    K: PartialEq,
    F: Fn(&T) -> K,
{
    let item_key = key(&item);
    if let Some(existing) = items.iter_mut().find(|existing| key(existing) == item_key) {
        *existing = item;
    } else {
        items.push(item);
    }
}

/// Upserts a privacy-projected user by Snowflake.
fn upsert_user(items: &mut Vec<UserPresence>, item: UserPresence) {
    upsert_by_id(items, item, |user| user.user_id);
}

/// Upserts voice membership while enforcing one active channel per user.
fn upsert_membership(items: &mut Vec<VoiceMembership>, item: VoiceMembership) {
    if let Some(existing) = items
        .iter_mut()
        .find(|existing| existing.user_id == item.user_id)
    {
        *existing = item;
    } else {
        items.push(item);
    }
}

#[derive(Debug, Deserialize)]
struct EntityTombstone {
    #[serde(alias = "group_id", alias = "channel_id")]
    entity_id: zephyrvox_types::Snowflake,
}

#[derive(Debug, Deserialize)]
struct UserTombstone {
    user_id: zephyrvox_types::Snowflake,
}

#[derive(Debug, Deserialize)]
struct RoleTombstone {
    role_key: String,
}

#[derive(Debug, Deserialize)]
struct UserEnvelope {
    user: UserPresence,
}

#[derive(Debug, Deserialize)]
struct SelfEnvelope {
    #[serde(rename = "self")]
    self_state: SelfUserState,
}

#[derive(Debug, Deserialize)]
struct MembershipEnvelope {
    membership: VoiceMembership,
}

#[derive(Debug, Deserialize)]
struct MembershipTombstone {
    user_id: zephyrvox_types::Snowflake,
    channel_id: zephyrvox_types::Snowflake,
}

#[derive(Debug, Deserialize)]
struct AuthorityEnvelope {
    authority: Option<VoiceAuthority>,
}

/// Returns whether the SDK has an explicit projection rule for an event.
///
/// This list is intentionally closed. A new replayable state event must be
/// added here and to `apply_known_event` together, otherwise the client asks
/// for a snapshot instead of silently losing convergence.
pub(crate) fn is_known_state_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "group.created"
            | "group.updated"
            | "group.deleted"
            | "group.access.updated"
            | "channel.created"
            | "channel.updated"
            | "channel.deleted"
            | "channel.access.updated"
            | "channel.member.joined"
            | "channel.member.left"
            | "user.created"
            | "user.updated"
            | "user.deleted"
            | "presence.updated"
            | "self.updated"
            | "rbac.role.created"
            | "rbac.role.updated"
            | "rbac.role.deleted"
            | "rbac.binding.updated"
            | "rbac.config.updated"
            | "owner.transferred"
            | "moderation.mute.updated"
            | "moderation.mute.removed"
            | "voice.authority.updated"
            | "voice.revoked"
            | "voice.disconnected"
            | "voice.membership.updated"
            | "server.updated"
            | "visibility.grant.begin"
            | "visibility.fragment"
            | "visibility.granted"
            | "visibility.revoked"
            | "visibility.tombstone"
            | "visibility.transition.complete"
    )
}
