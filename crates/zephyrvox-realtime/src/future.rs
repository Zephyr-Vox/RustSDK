use std::{future::Future, pin::Pin};

/// An owned, sendable future used by realtime provider and transport traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
