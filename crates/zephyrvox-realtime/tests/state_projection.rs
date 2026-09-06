mod support;

use support::{sample_channel, sample_group, snapshot, state_event};
use zephyrvox_realtime::{StateApplyError, StateStore};

#[tokio::test]
async fn state_events_replace_entities_atomically_and_publish_ordered_events() {
    let store = StateStore::from_snapshot(snapshot());
    let mut events = store.subscribe_events();
    let group = sample_group();
    store
        .apply_event(state_event(
            1,
            "group.created",
            serde_json::to_value(&group).unwrap(),
        ))
        .await
        .unwrap();
    let channel = sample_channel();
    store
        .apply_event(state_event(
            3,
            "channel.created",
            serde_json::to_value(&channel).unwrap(),
        ))
        .await
        .unwrap();

    let state = store.snapshot().await;
    assert_eq!(state.geid.get(), 3);
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

    let known = state_event(2, "server.updated", serde_json::json!({}));
    store.apply_event(known.clone()).await.unwrap();
    let backwards = state_event(2, "server.updated", serde_json::json!({}));
    assert!(matches!(
        store.apply_event(backwards).await,
        Err(StateApplyError::GeidOrder { .. })
    ));
    assert_eq!(store.snapshot().await.cursor, known.cursor);
}
