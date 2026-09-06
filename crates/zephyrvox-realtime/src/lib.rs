//! Tokio-first WebSocket control-plane synchronization for CommunityServer.
//!
//! The crate deliberately separates the protocol parser, ordered state
//! projection, synchronization state machine, transport adapter, and
//! connection coordinator. Applications normally use [`ControlConnection`]
//! while tests and alternative hosts can exercise [`StateStore`],
//! [`SyncMachine`], and [`WebSocketConnector`] independently.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod command;
mod config;
mod connection;
mod engine;
mod error;
mod event;
mod frame;
mod future;
mod provider;
mod state;
mod sync;
mod tls;
mod token;
mod transport;

pub use command::{CommandAck, PresenceCommand};
pub use config::{ConnectionStatus, RealtimeConfig, ReconnectPolicy};
pub use connection::ControlConnection;
pub use error::{CommandError, RealtimeError, StateApplyError, SyncError};
pub use event::ClientEvent;
pub use frame::{
    AuthRevoked, ConnectionReady, ServerFrame, SyncComplete, SyncReplay, SyncRequired,
    UnknownFrame, parse_server_frame,
};
pub use future::BoxFuture;
pub use provider::SnapshotProvider;
pub use state::{ApplyOutcome, StateStore};
pub use sync::{SyncMachine, SyncPhase};
pub use token::AccessToken;
pub use transport::{
    TokioTungsteniteConnector, WebSocketConnector, WebSocketMessage, WebSocketSession,
};
