use std::{future::Future, pin::Pin};

/// An owned, sendable future used by object-safe SDK traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
