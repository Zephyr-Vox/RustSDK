//! Runs a two-client interoperability check against a live CommunityServer.

use std::{env, error::Error, io, str::FromStr, sync::Arc, time::Duration};

use tokio::sync::watch;
use zephyrvox_sdk::{
    Client, ClientConfig, ClientEvent, CreateChannelRequest, EventStream, LoginRequest, Presence,
    RegisterRequest, RequestOptions, ServerCard, Snowflake, VoiceJoinRequest,
};
use zephyrvox_types::ClientState;

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const MEDIA_PAYLOAD: &[u8] = b"rust-sdk-real-server-interop";

/// Connects two clients to CommunityServer and verifies control, state, voice,
/// and opaque media-frame forwarding.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let card = ServerCard::parse(&required("ZEPHYRVOX_SERVER_CARD")?)?;
    let owner_username = required("ZEPHYRVOX_USERNAME")?;
    let owner_password = required("ZEPHYRVOX_PASSWORD")?;
    let peer_username = required("ZEPHYRVOX_PEER_USERNAME")?;
    let peer_password = required("ZEPHYRVOX_PEER_PASSWORD")?;

    let owner = Client::new(ClientConfig::new(card.clone()))?;
    let peer = Client::new(ClientConfig::new(card))?;

    let result = run_interoperability(
        &owner,
        &peer,
        owner_username,
        owner_password,
        peer_username,
        peer_password,
    )
    .await;

    let peer_shutdown = peer.shutdown().await;
    let owner_shutdown = owner.shutdown().await;
    result?;
    peer_shutdown?;
    owner_shutdown?;
    println!("interoperability check passed");
    Ok(())
}

/// Performs the live protocol assertions while keeping cleanup in `main`.
async fn run_interoperability(
    owner: &Client,
    peer: &Client,
    owner_username: String,
    owner_password: String,
    peer_username: String,
    peer_password: String,
) -> Result<(), Box<dyn Error>> {
    let owner_metadata = owner.discover().await?;
    let peer_metadata = peer.discover().await?;
    if owner_metadata.protocol_version != peer_metadata.protocol_version {
        return Err(io::Error::other("clients received different protocol versions").into());
    }
    let codec = owner_metadata
        .codecs
        .first()
        .ok_or_else(|| io::Error::other("server advertised no voice codec"))?;
    let stream_type = owner_metadata
        .voice_stream_types
        .first()
        .map(|stream| stream.id)
        .ok_or_else(|| io::Error::other("server advertised no voice stream type"))?;
    println!(
        "metadata protocol={} codec={} {}Hz/{}ch stream={} voice={}:{}",
        owner_metadata.protocol_version,
        codec.name,
        codec.sample_rate(),
        codec.channels,
        stream_type.get(),
        owner_metadata.voice_endpoint.host,
        owner_metadata.voice_endpoint.port
    );

    let owner_login = owner
        .login(LoginRequest {
            username: owner_username,
            password: owner_password,
            device_id: "rust-sdk-interop-owner".to_owned(),
        })
        .await?;
    if env::var("ZEPHYRVOX_REGISTER_PEER").is_ok_and(|value| value == "1") {
        owner
            .api()
            .auth()
            .register(RegisterRequest {
                username: peer_username.clone(),
                password: peer_password.clone(),
                nickname: Some("Rust SDK Interop Peer".to_owned()),
                invite: None,
            })
            .await?;
        println!("registered peer account {peer_username}");
    }
    peer.login(LoginRequest {
        username: peer_username.clone(),
        password: peer_password,
        device_id: "rust-sdk-interop-peer".to_owned(),
    })
    .await?;

    let channel_id = match env::var("ZEPHYRVOX_CHANNEL_ID") {
        Ok(value) => Snowflake::from_str(&value)?,
        Err(_) => {
            owner
                .api()
                .channels()
                .create(
                    CreateChannelRequest {
                        group_id: None,
                        name: "rust-sdk-interop".to_owned(),
                        mode: "voice".to_owned(),
                        temporary: true,
                        visibility: "public".to_owned(),
                        capacity: Some(8),
                        position: 0,
                        pinned: false,
                    },
                    RequestOptions::mutation(),
                )
                .await?
                .data
                .id
        }
    };
    println!("using voice channel {channel_id}");

    let owner_control = owner.connect().await?;
    let peer_control = peer.connect().await?;
    owner_control.wait_until_live().await?;
    peer_control.wait_until_live().await?;
    println!(
        "both control connections are live owner={:?} peer={:?}",
        owner_control.status(),
        peer_control.status()
    );
    let mut owner_events = owner.events();
    let mut peer_events = peer.events();

    let peer_state = peer
        .state()
        .ok_or_else(|| io::Error::other("peer state is unavailable after connect"))?;
    println!("before presence owner={:?}", owner_control.status());
    owner
        .set_presence(Presence {
            status: "dnd".to_owned(),
            activity: None,
        })
        .await?;
    wait_for_user_presence(peer_state, owner_login.user.id, "dnd").await?;
    println!("presence.set propagated to the peer state projection");

    let owner_voice = owner
        .join_voice(
            channel_id,
            VoiceJoinRequest {
                device_id: Some("rust-sdk-interop-owner".to_owned()),
                ..VoiceJoinRequest::default()
            },
        )
        .await?;
    let peer_voice = peer
        .join_voice(
            channel_id,
            VoiceJoinRequest {
                device_id: Some("rust-sdk-interop-peer".to_owned()),
                ..VoiceJoinRequest::default()
            },
        )
        .await?;
    if owner_voice.codec() != peer_voice.codec()
        || owner_voice.max_payload() != peer_voice.max_payload()
        || owner_voice.encrypted() != peer_voice.encrypted()
    {
        return Err(io::Error::other("voice negotiations disagree").into());
    }
    println!(
        "voice joined codec={} encrypted={} max_payload={}",
        owner_voice.codec().name,
        owner_voice.encrypted(),
        owner_voice.max_payload()
    );
    tokio::task::yield_now().await;
    print_sync_diagnostics("owner", &mut owner_events);
    print_sync_diagnostics("peer", &mut peer_events);
    println!(
        "voice status owner={:?} peer={:?}",
        owner_voice.status(),
        peer_voice.status()
    );
    println!(
        "voice identity owner_session={} owner_control={:?} peer_session={} peer_control={:?}",
        owner_voice.session_id(),
        owner_control.id(),
        peer_voice.session_id(),
        peer_control.id()
    );

    owner_voice.send_encoded(stream_type, MEDIA_PAYLOAD).await?;
    let received = tokio::time::timeout(WAIT_TIMEOUT, peer_voice.recv()).await??;
    if received.speaker_id != owner_login.user.id {
        return Err(io::Error::other("forwarded frame has the wrong speaker").into());
    }
    if received.stream_type != stream_type || received.payload.as_ref() != MEDIA_PAYLOAD {
        return Err(io::Error::other("forwarded frame payload or stream type changed").into());
    }
    println!(
        "media forwarded speaker={} stream={} bytes={} channel_seq={} transport_seq={}",
        received.speaker_id,
        received.stream_type.get(),
        received.payload.len(),
        received.channel_seq,
        received.transport_seq
    );

    if let Err(error) = peer.leave_voice().await {
        println!("peer leave failed: {error}");
        return Err(error.into());
    }
    if let Err(error) = owner.leave_voice().await {
        println!("owner leave failed: {error}");
        return Err(error.into());
    }
    Ok(())
}

/// Waits until a visible user's presence reaches the expected status.
async fn wait_for_user_presence(
    mut state: watch::Receiver<Arc<ClientState>>,
    user_id: Snowflake,
    expected_status: &str,
) -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if state
                .borrow()
                .users
                .iter()
                .any(|user| user.user_id == user_id && user.presence.status == expected_status)
            {
                return Ok::<(), watch::error::RecvError>(());
            }
            state.changed().await?;
        }
    })
    .await??;
    Ok(())
}

/// Returns one required environment variable with an actionable error.
fn required(name: &str) -> Result<String, io::Error> {
    env::var(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))
}

/// Prints synchronization diagnostics observed while an HTTP mutation is
/// being reflected into each WebSocket state projection.
fn print_sync_diagnostics(label: &str, events: &mut EventStream) {
    while let Ok(event) = events.try_recv() {
        match event {
            ClientEvent::SyncRequired { reason } => {
                println!("{label} requested snapshot fallback: {reason}");
            }
            ClientEvent::Error { message } => {
                println!("{label} facade diagnostic: {message}");
            }
            _ => {}
        }
    }
}
