//! Shared test-only helpers for protocol and transport integration tests.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod fixtures;

pub use fixtures::{FixtureError, FixtureLoader};
