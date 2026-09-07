//! Shared test-only helpers for protocol and transport integration tests.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod fixtures;
mod http;
mod policy;
mod realtime;
mod udp;

pub use fixtures::{FixtureError, FixtureLoader};
pub use http::FakeHttpTransport;
pub use policy::deterministic_reconnect_policy;
pub use realtime::{
    FakeWebSocketConnector, FakeWebSocketPeer, FakeWebSocketSession, websocket_pair,
};
pub use udp::UdpPair;
