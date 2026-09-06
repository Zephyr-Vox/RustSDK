#![allow(dead_code)]

use std::{collections::VecDeque, str::FromStr, sync::Arc};

use tokio::sync::{Mutex, mpsc};

use zephyrvox_realtime::{
    AccessToken, BoxFuture, RealtimeError, SnapshotProvider, WebSocketConnector, WebSocketMessage,
    WebSocketSession,
};

use zephyrvox_types::{
    Channel, ClientState, ControlConnectionId, Cursor, Geid, Group, Presence, Role, SelfUserState,
    ServerState, SnapshotState, Snowflake, StateEvent, StateSnapshot, StreamEpoch, User,
};

pub struct FakeProvider;

impl SnapshotProvider for FakeProvider {
    fn snapshot(&self) -> BoxFuture<'static, Result<StateSnapshot, RealtimeError>> {
        Box::pin(async { Ok(snapshot()) })
    }

    fn access_token(&self) -> BoxFuture<'static, Result<AccessToken, RealtimeError>> {
        Box::pin(async { AccessToken::new("access-1") })
    }
}

pub struct FakeSession {
    pub incoming: mpsc::Receiver<WebSocketMessage>,
    pub outgoing: mpsc::Sender<String>,
}

impl WebSocketSession for FakeSession {
    fn send_text<'a>(&'a mut self, text: String) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async move {
            self.outgoing
                .send(text)
                .await
                .map_err(|_| RealtimeError::Closed)
        })
    }

    fn send_pong<'a>(&'a mut self, _payload: Vec<u8>) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<WebSocketMessage>, RealtimeError>> {
        Box::pin(async move { Ok(self.incoming.recv().await) })
    }

    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async { Ok(()) })
    }
}

pub struct FakeConnector {
    pub sessions: Arc<Mutex<VecDeque<FakeSession>>>,
}

impl WebSocketConnector for FakeConnector {
    fn connect(
        &self,
        _url: url::Url,
        _access_token: AccessToken,
    ) -> BoxFuture<'static, Result<Box<dyn WebSocketSession>, RealtimeError>> {
        let session = Arc::clone(&self.sessions);
        Box::pin(async move {
            let mut sessions = session.lock().await;
            let session = sessions.pop_front().ok_or(RealtimeError::Closed)?;
            Ok(Box::new(session) as Box<dyn WebSocketSession>)
        })
    }
}

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
