use serde_json::Value;
use zephyrvox_wire::{ApiEnvelope, EnvelopeError};

#[test]
fn global_validation_errors_expose_field_messages() {
    let envelope: ApiEnvelope<Value> =
        serde_json::from_str(include_str!("fixtures/error-envelope.json")).expect("envelope");
    let error = envelope.into_result().expect_err("error envelope");
    match error {
        EnvelopeError::Api(error) => {
            assert_eq!(error.code, 1000);
            assert_eq!(
                error.fields.get("username").map(String::as_str),
                Some("must not be empty")
            );
        }
        other => panic!("unexpected envelope error: {other:?}"),
    }
}

#[test]
fn successful_value_envelope_can_be_decoded_into_a_typed_body() {
    let envelope: ApiEnvelope<Value> =
        serde_json::from_str(include_str!("fixtures/success-envelope.json")).expect("envelope");
    let data: ExampleData = envelope.into_typed().expect("typed response");
    assert_eq!(data.id, "123");
    assert!(data.enabled);
}

#[test]
fn success_without_data_is_rejected() {
    let envelope = ApiEnvelope::<Value> {
        code: 0,
        message: String::new(),
        data: None,
    };
    assert!(matches!(
        envelope.into_result(),
        Err(EnvelopeError::MissingSuccessData)
    ));
}

#[test]
fn success_with_a_message_is_rejected() {
    let envelope = ApiEnvelope::<Value> {
        code: 0,
        message: "unexpected status".to_owned(),
        data: Some(Value::Null),
    };
    assert!(matches!(
        envelope.into_result(),
        Err(EnvelopeError::NonEmptySuccessMessage)
    ));
}

#[derive(Debug, serde::Deserialize)]
struct ExampleData {
    id: String,
    enabled: bool,
}
