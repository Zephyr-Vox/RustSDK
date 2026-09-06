mod support;

use support::{sample_channel, sample_group, snapshot, state_event};
use zephyrvox_realtime::{StateApplyError, StateStore};
use zephyrvox_types::{SessionId, Snowflake, VoiceAuthority};

#[tokio::test]
async fn state_events_replace_entities_atomically_and_publish_ordered_events() {
    let store = StateStore::from_snapshot(snapshot());
    let mut events = store.subscribe_events();
    let group = sample_group();
    let mut group_event = state_event(1, "group.created", serde_json::to_value(&group).unwrap());
    group_event.scope = zephyrvox_types::EventScope {
        kind: "group".to_owned(),
        id: Some(group.id),
    };
    store.apply_event(group_event).await.unwrap();
    let channel = sample_channel();
    let mut channel_event = state_event(
        2,
        "channel.created",
        serde_json::to_value(&channel).unwrap(),
    );
    channel_event.scope = zephyrvox_types::EventScope {
        kind: "channel".to_owned(),
        id: Some(channel.id),
    };
    store.apply_event(channel_event).await.unwrap();

    let state = store.snapshot().await;
    assert_eq!(state.geid.get(), 2);
    assert_eq!(state.groups, vec![group]);
    assert_eq!(state.channels, vec![channel]);
    assert_eq!(events.recv().await.unwrap().event_type, "group.created");
    assert_eq!(events.recv().await.unwrap().event_type, "channel.created");
}

#[tokio::test]
async fn unknown_or_backwards_events_do_not_mutate_the_projection() {
    let store = StateStore::from_snapshot(snapshot());
    let unknown = state_event(1, "future.new_event", serde_json::json!({}));
    assert!(matches!(
        store.apply_event(unknown).await,
        Err(StateApplyError::UnknownEvent(_))
    ));
    assert_eq!(store.snapshot().await.geid.get(), 0);

    let known = state_event(1, "server.updated", serde_json::json!({}));
    store.apply_event(known.clone()).await.unwrap();
    let backwards = state_event(1, "server.updated", serde_json::json!({}));
    assert!(matches!(
        store.apply_event(backwards).await,
        Err(StateApplyError::GeidOrder { .. })
    ));
    assert_eq!(store.snapshot().await.cursor, known.cursor);
}

#[tokio::test]
async fn visibility_transitions_require_a_snapshot_before_publication() {
    let store = StateStore::from_snapshot(snapshot());
    let mut event = state_event(1, "visibility.grant.begin", serde_json::json!({}));
    event.scope = zephyrvox_types::EventScope {
        kind: "group".to_owned(),
        id: Some(zephyrvox_types::Snowflake::new(10).unwrap()),
    };
    assert!(matches!(
        store.apply_event(event).await,
        Err(StateApplyError::SnapshotRequired(event_type)) if event_type == "visibility.grant.begin"
    ));
    assert_eq!(store.snapshot().await.geid.get(), 0);
}

#[tokio::test]
async fn snapshot_replacement_discards_old_ordered_events() {
    let store = StateStore::from_snapshot(snapshot());
    let mut events = store.subscribe_events();
    store
        .apply_event(state_event(1, "server.updated", serde_json::json!({})))
        .await
        .unwrap();

    store.replace_snapshot(snapshot()).await;
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    store
        .apply_event(state_event(1, "server.updated", serde_json::json!({})))
        .await
        .unwrap();
    assert_eq!(events.recv().await.unwrap().geid.get(), 1);
}

#[tokio::test]
async fn new_event_subscribers_start_after_snapshot_replacement() {
    let store = StateStore::from_snapshot(snapshot());
    store
        .apply_event(state_event(1, "server.updated", serde_json::json!({})))
        .await
        .unwrap();
    store.replace_snapshot(snapshot()).await;

    let mut events = store.subscribe_events();
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn snapshot_replacement_reports_invalidated_voice_authority() {
    let mut old_snapshot = snapshot();
    let authority = VoiceAuthority {
        channel_id: Snowflake::new(11).unwrap(),
        control_connection_id: support::control_id(),
        voice_session_id: SessionId::parse_hex("000000000000000000000000000000aa").unwrap(),
        voice_authority_generation: "generation-1".to_owned(),
    };
    old_snapshot.state.self_state.voice_authority = Some(authority.clone());
    let store = StateStore::from_snapshot(old_snapshot);

    let lost = store.replace_snapshot(snapshot()).await;

    assert_eq!(lost, Some(authority));
}
