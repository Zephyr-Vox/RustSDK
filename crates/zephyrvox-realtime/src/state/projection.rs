use serde::Deserialize;

use crate::StateApplyError;
use zephyrvox_types::{
    Channel, ClientState, Geid, Group, Role, SelfUserState, StateEvent, UserPresence,
    VoiceAuthority, VoiceMembership,
};

/// Checks envelope, cursor, scope, GEID ordering, and the closed event table
/// before any projection field is cloned or changed.
pub(super) fn validate_event(
    event: &StateEvent,
    current: &ClientState,
) -> Result<(), StateApplyError> {
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
    let expected = Geid::new(current.geid.get().saturating_add(1));
    if event.geid != expected {
        return Err(StateApplyError::GeidGap {
            expected,
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
    if event.scope.kind == "server" && event.scope.id.is_some() {
        return Err(StateApplyError::InvalidEnvelope(
            "server event scope must not have an id".to_owned(),
        ));
    }
    if let Some(expected) = expected_scope_kind(&event.event_type) {
        if event.scope.kind != expected {
            return Err(StateApplyError::InvalidEnvelope(format!(
                "{} event must use a {} scope",
                event.event_type, expected
            )));
        }
    }
    if is_visibility_scope_event(&event.event_type) && event.scope.kind == "server" {
        return Err(StateApplyError::InvalidEnvelope(
            "visibility transition event must use a group or channel scope".to_owned(),
        ));
    }
    Ok(())
}

/// Applies the replace/upsert/delete semantics for one known event type.
pub(super) fn apply_known_event(
    state: &mut ClientState,
    event: &StateEvent,
) -> Result<(), StateApplyError> {
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
        | "acl.updated"
        | "channel.access.updated"
        | "rbac.binding.updated"
        | "rbac.config.updated"
        | "owner.transferred"
        | "moderation.mute.updated"
        | "moderation.mute.removed"
        | "server.updated" => {}
        event_type if is_visibility_state_event(event_type) => {
            return Err(StateApplyError::SnapshotRequired(event_type.to_owned()));
        }
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
fn is_known_state_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "group.created"
            | "group.updated"
            | "group.deleted"
            | "group.access.updated"
            | "acl.updated"
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

/// Returns the exact scope kind required by state events whose payload is
/// meaningful only for one resource family. ACL invalidations and moderation
/// events intentionally retain their broader server/group/channel scope.
fn expected_scope_kind(event_type: &str) -> Option<&'static str> {
    match event_type {
        "group.created" | "group.updated" | "group.deleted" | "group.access.updated" => {
            Some("group")
        }
        "channel.created"
        | "channel.updated"
        | "channel.deleted"
        | "channel.access.updated"
        | "channel.member.joined"
        | "channel.member.left"
        | "voice.membership.updated" => Some("channel"),
        "user.created"
        | "user.updated"
        | "user.deleted"
        | "presence.updated"
        | "self.updated"
        | "rbac.role.created"
        | "rbac.role.updated"
        | "rbac.role.deleted"
        | "owner.transferred"
        | "voice.authority.updated"
        | "voice.revoked"
        | "voice.disconnected"
        | "server.updated"
        | "visibility.transition.complete" => Some("server"),
        _ => None,
    }
}

/// Returns whether a visibility transition carries a group/channel scope.
fn is_visibility_scope_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "visibility.grant.begin"
            | "visibility.fragment"
            | "visibility.granted"
            | "visibility.revoked"
            | "visibility.tombstone"
    )
}

/// Returns whether the event must be completed from a fresh snapshot.
fn is_visibility_state_event(event_type: &str) -> bool {
    is_visibility_scope_event(event_type) || event_type == "visibility.transition.complete"
}
