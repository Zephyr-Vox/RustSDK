# zephyrvox-sdk

`zephyrvox-sdk` is the host-facing Tokio facade for CommunityServer. A host
creates one `Client`, calls `discover`/`login`, opens a control connection, and
uses the returned handles for state, events, presence, and voice. The facade
owns the HTTP refresh path, WebSocket synchronization task, reconnect lifecycle,
and UDP voice worker.

```rust,no_run
use zephyrvox_sdk::{Client, ClientConfig, ServerCard};

# async fn run(card: ServerCard) -> Result<(), zephyrvox_sdk::SdkError> {
let client = Client::new(ClientConfig::new(card))?;
let _metadata = client.discover().await?;
let control = client.connect().await?;
control.wait_until_live().await?;
let mut events = client.events();
// Feed `events.recv()` and `client.state()` into the host's own event loop.
client.shutdown().await?;
# Ok(())
# }
```

Tauri should keep `Client` in managed application state and run one event pump
that converts `EventStream` items and watch updates into frontend events. GPUI
can keep the client in its model state and schedule model updates from the
same pump. A TUI can poll `EventStream::try_recv` during its render tick while
using the watch receiver for the latest complete state.

Device capture, playback, Opus, jitter buffering, and platform permissions are
host concerns. Implement `MediaSource`, `MediaSink`, or `AudioCodec` in the
platform crate and pass encoded `OutboundMedia` frames to `VoiceSession`.
`InboundMedia` does not carry codec metadata; use `VoiceSession::codec()` from
the metadata negotiation when decoding it.

Call `Client::shutdown` from the host's application teardown path. Voice is
stopped before the control connection. Automatic voice rejoin is disabled by
default; hosts may opt in for ordinary reconnects with
`ClientConfig::with_auto_rejoin_voice(true)`, while authority loss and explicit
leave always clear the remembered join. After an automatic rejoin, await
`Client::voice_session()` to obtain the replacement handle.

For a live two-client interoperability check, run the opt-in example against a
CommunityServer instance:

```text
ZEPHYRVOX_SERVER_CARD=zephyrvox://127.0.0.1:28745?v=1 \
ZEPHYRVOX_USERNAME=owner \
ZEPHYRVOX_PASSWORD='owner password' \
ZEPHYRVOX_PEER_USERNAME=peer \
ZEPHYRVOX_PEER_PASSWORD='peer password' \
ZEPHYRVOX_REGISTER_PEER=1 \
cargo run -p zephyrvox-sdk --example interoperability
```

The example creates a temporary public voice channel when
`ZEPHYRVOX_CHANNEL_ID` is absent, verifies presence propagation, then sends
one opaque encoded frame and checks that the peer receives the same bytes.
`ZEPHYRVOX_REGISTER_PEER=1` is only needed when the peer account does not yet
exist.
