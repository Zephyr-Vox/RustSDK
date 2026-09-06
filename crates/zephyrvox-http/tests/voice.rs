mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use support::{FakeTransport, envelope};
use zephyrvox_http::{
    AccessToken, ApiClient, HttpError, RefreshToken, RequestOptions, TokenPair, VoiceJoinRequest,
};
use zephyrvox_types::{ControlConnectionId, Snowflake, VoiceSessionId};
use zephyrvox_wire::{ServerCard, TransportScheme};

fn plain_card() -> ServerCard {
    ServerCard::new("example.test", 80, TransportScheme::Plain, None, None).expect("card")
}

fn id(value: u64) -> Snowflake {
    Snowflake::new(value).expect("snowflake")
}

fn tokens() -> TokenPair {
    TokenPair::new(
        AccessToken::new("access").expect("access"),
        RefreshToken::new("refresh").expect("refresh"),
        Duration::from_secs(300),
    )
    .expect("tokens")
}

fn control_connection() -> ControlConnectionId {
    ControlConnectionId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        .expect("control connection")
}

#[tokio::test(flavor = "current_thread")]
async fn voice_join_requires_control_connection_and_parses_negotiation() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.url().path(), "/api/v0/channels/102/join");
        assert_eq!(
            request.header("x-zephyr-control-connection"),
            Some("0102030405060708090a0b0c0d0e0f10")
        );
        assert!(request.header("idempotency-key").is_some());
        Ok(envelope(serde_json::json!({
            "channel": {
                "id": "102",
                "group_id": null,
                "name": "Voice",
                "mode": "voice",
                "temporary": false,
                "visibility": "public",
                "capacity": 32,
                "position": 1,
                "pinned": false,
                "version": "1"
            },
            "members": [{"user_id":"7","channel_id":"102","joined_at":1000}],
            "voice": {
                "created": true,
                "session_id": "00112233445566778899aabbccddeeff",
                "key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "encrypted": true,
                "max_payload": 1142,
                "protocol_version": 1,
                "expires_at": 2000
            },
            "state_cursor": "cursor-1",
            "state_checkpoint": {
                "stream_epoch": "00112233445566778899aabbccddeeff",
                "geid": "9"
            }
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    let no_connection = client
        .channels()
        .join_voice(id(102), VoiceJoinRequest::default(), RequestOptions::new())
        .await;
    assert!(matches!(
        no_connection,
        Err(HttpError::MissingControlConnection)
    ));

    let joined = client
        .channels()
        .join_voice(
            id(102),
            VoiceJoinRequest {
                device_id: Some("desktop".to_owned()),
                force_new: false,
                expected_voice_session_id: Some(
                    VoiceSessionId::parse_hex("00112233445566778899aabbccddeeff")
                        .expect("voice id"),
                ),
            },
            RequestOptions::new().with_control_connection(control_connection()),
        )
        .await
        .expect("join");
    assert_eq!(joined.data.voice.max_payload, 1142);
    assert_eq!(joined.data.state_checkpoint.geid.get(), 9);
    assert_eq!(joined.data.members[0].user_id, id(7));
}

#[tokio::test(flavor = "current_thread")]
async fn voice_join_rejects_invalid_session_key_lifecycle() {
    let call = Arc::new(AtomicUsize::new(0));
    let call_for_handler = Arc::clone(&call);
    let transport = FakeTransport::new(move |_| {
        let created = call_for_handler.fetch_add(1, Ordering::SeqCst) == 1;
        Ok(envelope(serde_json::json!({
            "channel": {
                "id": "102",
                "group_id": null,
                "name": "Voice",
                "mode": "voice",
                "temporary": false,
                "visibility": "public",
                "capacity": 32,
                "position": 1,
                "pinned": false,
                "version": "1"
            },
            "members": [],
            "voice": {
                "created": created,
                "session_id": "00112233445566778899aabbccddeeff",
                "key": "not-base64-key",
                "encrypted": true,
                "max_payload": 1142,
                "protocol_version": 1,
                "expires_at": 2000
            },
            "state_cursor": "cursor-1",
            "state_checkpoint": {
                "stream_epoch": "00112233445566778899aabbccddeeff",
                "geid": "9"
            }
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    let first = client
        .channels()
        .join_voice(
            id(102),
            VoiceJoinRequest::default(),
            RequestOptions::new().with_control_connection(control_connection()),
        )
        .await;
    assert!(matches!(first, Err(HttpError::Wire(_))));

    let second = client
        .channels()
        .join_voice(
            id(102),
            VoiceJoinRequest::default(),
            RequestOptions::new().with_control_connection(control_connection()),
        )
        .await;
    assert!(matches!(second, Err(HttpError::Wire(_))));
}

#[tokio::test(flavor = "current_thread")]
async fn voice_join_rejects_a_response_for_another_channel() {
    let transport = FakeTransport::new(|_| {
        Ok(envelope(serde_json::json!({
            "channel": {
                "id": "103",
                "group_id": null,
                "name": "Other voice",
                "mode": "voice",
                "temporary": false,
                "visibility": "public",
                "capacity": 32,
                "position": 1,
                "pinned": false,
                "version": "1"
            },
            "members": [],
            "voice": {
                "created": false,
                "session_id": "00112233445566778899aabbccddeeff",
                "encrypted": false,
                "warning": "plaintext_mode",
                "max_payload": 1158,
                "protocol_version": 1,
                "expires_at": 2000
            },
            "state_cursor": "cursor-1",
            "state_checkpoint": {
                "stream_epoch": "00112233445566778899aabbccddeeff",
                "geid": "9"
            }
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    client.set_tokens(tokens()).await.expect("tokens");

    assert!(matches!(
        client
            .channels()
            .join_voice(
                id(102),
                VoiceJoinRequest::default(),
                RequestOptions::new().with_control_connection(control_connection()),
            )
            .await,
        Err(HttpError::Wire(_))
    ));
}
