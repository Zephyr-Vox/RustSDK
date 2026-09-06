mod support;

use std::sync::Arc;

use support::{FakeTransport, envelope};
use zephyrvox_http::{ApiClient, HttpError};
use zephyrvox_wire::{ServerCard, TransportScheme};

fn plain_card() -> ServerCard {
    ServerCard::new("example.test", 80, TransportScheme::Plain, None, None).expect("card")
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_is_public_and_checks_the_single_protocol_version() {
    let transport = FakeTransport::new(|request| {
        assert_eq!(request.url().path(), "/api/v0/metadata");
        assert!(request.header("authorization").is_none());
        Ok(envelope(serde_json::json!({
            "protocol_version": 1,
            "voice_endpoint": {"host": "voice.example.test", "port": 40100},
            "codecs": [{
                "name": "opus",
                "clock_rate": 48000,
                "channels": 2,
                "ptime": [10, 20, 40, 60]
            }],
            "voice_stream_types": [{
                "id": 1,
                "name": "microphone",
                "mute_kind": "voice"
            }],
            "features": []
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    let metadata = client.metadata().await.expect("metadata");
    assert_eq!(metadata.codecs[0].sample_rate(), 48_000);
    assert_eq!(metadata.voice_endpoint.port, 40_100);
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_rejects_a_future_protocol_without_fallback() {
    let transport = FakeTransport::new(|_| {
        Ok(envelope(serde_json::json!({
            "protocol_version": 2,
            "voice_endpoint": {"host": "voice.example.test", "port": 40100},
            "codecs": [],
            "voice_stream_types": [],
            "features": []
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    assert!(matches!(
        client.metadata().await,
        Err(HttpError::ProtocolVersionMismatch {
            expected: 1,
            actual: 2
        })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_rejects_wildcard_voice_endpoints() {
    let transport = FakeTransport::new(|_| {
        Ok(envelope(serde_json::json!({
            "protocol_version": 1,
            "voice_endpoint": {"host": "0.0.0.0", "port": 40100},
            "codecs": [{
                "name": "opus",
                "clock_rate": 48000,
                "channels": 2,
                "ptime": [20]
            }],
            "voice_stream_types": [{
                "id": 1,
                "name": "microphone",
                "mute_kind": "voice"
            }],
            "features": []
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    assert!(matches!(client.metadata().await, Err(HttpError::Wire(_))));
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_rejects_ambiguous_capability_labels() {
    let transport = FakeTransport::new(|_| {
        Ok(envelope(serde_json::json!({
            "protocol_version": 1,
            "voice_endpoint": {"host": "voice.example.test", "port": 40100},
            "codecs": [{
                "name": " opus ",
                "clock_rate": 48000,
                "channels": 2,
                "ptime": [20]
            }],
            "voice_stream_types": [{
                "id": 1,
                "name": "microphone",
                "mute_kind": "voice"
            }],
            "features": []
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    assert!(matches!(client.metadata().await, Err(HttpError::Wire(_))));
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_rejects_invalid_feature_names() {
    let transport = FakeTransport::new(|_| {
        Ok(envelope(serde_json::json!({
            "protocol_version": 1,
            "voice_endpoint": {"host": "voice.example.test", "port": 40100},
            "codecs": [{
                "name": "opus",
                "clock_rate": 48000,
                "channels": 2,
                "ptime": [20]
            }],
            "voice_stream_types": [{
                "id": 1,
                "name": "microphone",
                "mute_kind": "voice"
            }],
            "features": [" voice.join"]
        })))
    });
    let client =
        ApiClient::with_memory_transport(plain_card(), Arc::new(transport)).expect("client");
    assert!(matches!(client.metadata().await, Err(HttpError::Wire(_))));
}
