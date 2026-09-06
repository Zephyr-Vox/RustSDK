use zephyrvox_types::{CodecCapability, Metadata, StreamTypeId};

#[test]
fn metadata_preserves_codec_sampling_information() {
    let metadata: Metadata =
        serde_json::from_str(include_str!("fixtures/metadata.json")).expect("metadata fixture");
    let opus = metadata
        .codecs
        .iter()
        .find(|codec| codec.name == "opus")
        .expect("opus capability");
    assert_eq!(opus.sample_rate(), 48_000);
    assert_eq!(opus.channels, 2);
    assert_eq!(opus.ptime, vec![10, 20, 40, 60]);
    assert_eq!(metadata.voice_stream_types[0].id, StreamTypeId::new(1));
}

#[test]
fn codec_sample_rate_is_an_alias_for_clock_rate() {
    let codec = CodecCapability {
        name: "opus".to_owned(),
        clock_rate: 48_000,
        channels: 2,
        ptime: vec![20],
    };
    assert_eq!(codec.sample_rate(), codec.clock_rate);
}
