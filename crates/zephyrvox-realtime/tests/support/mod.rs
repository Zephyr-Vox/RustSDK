#![allow(dead_code)]

use std::str::FromStr;

use zephyrvox_types::{
    Channel, ClientState, ControlConnectionId, Cursor, Geid, Group, Presence, Role, SelfUserState,
    ServerState, SnapshotState, Snowflake, StateEvent, StateSnapshot, StreamEpoch, User,
};

pub fn snapshot() -> StateSnapshot {
    let user_id = Snowflake::new(1).unwrap();
    StateSnapshot {
        cursor: Cursor::new("cursor-0").unwrap(),
        stream_epoch: StreamEpoch::parse_hex("00000000000000000000000000000001").unwrap(),
        geid: Geid::new(0),
        state_version: "1".to_owned(),
        state: SnapshotState {
            server: ServerState {
                name: "Test".to_owned(),
                version: "dev".to_owned(),
            },
            self_state: SelfUserState {
                user: User {
                    id: user_id,
                    username: "owner".to_owned(),
                    nickname: "Owner".to_owned(),
                    avatar: None,
                },
                server_role_keys: vec!["owner".to_owned()],
                server_permissions: vec!["*".to_owned()],
                presence: Presence {
                    status: "online".to_owned(),
                    activity: None,
                },
                voice_authority: None,
            },
            users: Vec::new(),
            roles: vec![Role {
                key: "owner".to_owned(),
                display_name: "Owner".to_owned(),
                rank: 0,
                builtin: true,
            }],
            groups: Vec::new(),
            channels: Vec::new(),
            voice_memberships: Vec::new(),
        },
    }
}

pub fn state_event(geid: u64, event_type: &str, data: serde_json::Value) -> StateEvent {
    StateEvent {
        frame_type: "state.event".to_owned(),
        geid: Geid::new(geid),
        cursor: Cursor::new(format!("cursor-{geid}")).unwrap(),
        class: "state".to_owned(),
        scope: zephyrvox_types::EventScope {
            kind: "server".to_owned(),
            id: None,
        },
        event_type: event_type.to_owned(),
        causation_id: None,
        server_time: 1_700_000_000_000 + geid as i64,
        data,
    }
}

pub fn control_id() -> ControlConnectionId {
    ControlConnectionId::parse_hex("00000000000000000000000000000011").unwrap()
}

pub fn epoch() -> StreamEpoch {
    StreamEpoch::from_str("00000000000000000000000000000001").unwrap()
}

#[allow(dead_code)]
pub fn empty_client_state() -> ClientState {
    ClientState::from_snapshot(snapshot())
}

#[allow(dead_code)]
pub fn sample_group() -> Group {
    Group {
        id: Snowflake::new(10).unwrap(),
        name: "General".to_owned(),
        position: 1,
        visibility: "public".to_owned(),
        version: "2".to_owned(),
    }
}

#[allow(dead_code)]
pub fn sample_channel() -> Channel {
    Channel {
        id: Snowflake::new(11).unwrap(),
        group_id: Some(Snowflake::new(10).unwrap()),
        name: "Lobby".to_owned(),
        mode: "text".to_owned(),
        temporary: false,
        visibility: "public".to_owned(),
        capacity: 0,
        position: 1,
        pinned: false,
        version: "1".to_owned(),
    }
}
