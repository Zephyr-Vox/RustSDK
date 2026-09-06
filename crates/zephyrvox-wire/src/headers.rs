/// Bearer access-token header.
pub const AUTHORIZATION: &str = "Authorization";
/// Mutation idempotency key header.
pub const IDEMPOTENCY_KEY: &str = "Idempotency-Key";
/// Resource ETag precondition header.
pub const IF_MATCH: &str = "If-Match";
/// Parent resource ETag precondition header.
pub const PARENT_IF_MATCH: &str = "X-Zephyr-Parent-If-Match";
/// Active WebSocket control connection identifier.
pub const CONTROL_CONNECTION: &str = "X-Zephyr-Control-Connection";
/// Server command identifier returned by a committed mutation.
pub const COMMAND_ID: &str = "X-Zephyr-Command-ID";
/// Authenticated state cursor returned by a committed mutation.
pub const STATE_CURSOR: &str = "X-Zephyr-State-Cursor";
/// State stream epoch returned by a committed mutation.
pub const STREAM_EPOCH: &str = "X-Zephyr-Stream-Epoch";
/// Highest committed state GEID returned by a mutation.
pub const GEID: &str = "X-Zephyr-Geid";
/// Indicates that the client must obtain a replacement snapshot.
pub const SYNC_REQUIRED: &str = "X-Zephyr-Sync-Required";
