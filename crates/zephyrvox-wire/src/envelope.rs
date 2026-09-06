use std::collections::BTreeMap;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;

/// The JSON envelope used by every body-bearing HTTP response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ApiEnvelope<T> {
    /// Zero for success, endpoint-local or global error code otherwise.
    pub code: i32,
    /// Human-readable status message.
    pub message: String,
    /// Typed success data, or null for the usual error response.
    pub data: Option<T>,
}

impl<T> ApiEnvelope<T> {
    /// Creates a successful response envelope.
    pub fn success(data: T) -> Self {
        Self {
            code: 0,
            message: String::new(),
            data: Some(data),
        }
    }

    /// Reports whether the envelope has the protocol success code.
    pub const fn is_success(&self) -> bool {
        self.code == 0
    }

    /// Maps a successful optional body into another typed envelope.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> ApiEnvelope<U> {
        ApiEnvelope {
            code: self.code,
            message: self.message,
            data: self.data.map(map),
        }
    }
}

impl<T> ApiEnvelope<T>
where
    T: Serialize,
{
    /// Converts a response envelope into a typed result.
    ///
    /// Error data is inspected for the global validation shape
    /// data.fields, while successful data must be present.
    pub fn into_result(self) -> Result<T, EnvelopeError> {
        if self.code == 0 {
            return self.data.ok_or(EnvelopeError::MissingSuccessData);
        }

        let data = self
            .data
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| EnvelopeError::DataSerialization(error.to_string()))?;
        let fields = extract_field_errors(data.as_ref());
        Err(EnvelopeError::Api(ApiError {
            http_status: None,
            endpoint: None,
            code: self.code,
            message: self.message,
            fields,
            retryable: false,
        }))
    }
}

impl ApiEnvelope<Value> {
    /// Decodes a successful envelope's data into a concrete response type.
    pub fn into_typed<T: DeserializeOwned>(self) -> Result<T, EnvelopeError> {
        let value = self.into_result()?;
        serde_json::from_value(value)
            .map_err(|error| EnvelopeError::DataDeserialization(error.to_string()))
    }
}

fn extract_field_errors(data: Option<&Value>) -> BTreeMap<String, String> {
    let Some(fields) = data
        .and_then(Value::as_object)
        .and_then(|object| object.get("fields"))
        .and_then(Value::as_object)
    else {
        return BTreeMap::new();
    };

    fields
        .iter()
        .filter_map(|(field, message)| {
            message
                .as_str()
                .map(|message| (field.clone(), message.to_owned()))
        })
        .collect()
}

/// The normalized API error returned by an unsuccessful envelope.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("API error {code}: {message}")]
pub struct ApiError {
    /// HTTP status when the transport layer has attached response context.
    pub http_status: Option<u16>,
    /// Request path or endpoint name when known.
    pub endpoint: Option<String>,
    /// Endpoint-local or global error code.
    pub code: i32,
    /// Human-readable server message.
    pub message: String,
    /// Field-level messages for global code 1000 responses.
    pub fields: BTreeMap<String, String>,
    /// Whether retrying the same logical operation is safe by contract.
    pub retryable: bool,
}

impl ApiError {
    /// Attaches HTTP context without changing the endpoint business code.
    pub fn with_http_context(
        mut self,
        http_status: u16,
        endpoint: impl Into<String>,
        retryable: bool,
    ) -> Self {
        self.http_status = Some(http_status);
        self.endpoint = Some(endpoint.into());
        self.retryable = retryable;
        self
    }
}

/// Errors raised while interpreting an HTTP response envelope.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeError {
    /// The server returned a non-zero API code.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// A success envelope omitted its data value.
    #[error("successful API envelope omitted data")]
    MissingSuccessData,
    /// Error data could not be converted to JSON for inspection.
    #[error("could not serialize API error data: {0}")]
    DataSerialization(String),
    /// Successful JSON data could not be decoded into the requested type.
    #[error("could not deserialize API response data: {0}")]
    DataDeserialization(String),
}
