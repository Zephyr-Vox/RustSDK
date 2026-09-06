use std::str::FromStr;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{errors::HttpError, request::HttpResponse};
use zephyrvox_types::{Cursor, Etag, Geid, StreamEpoch};
use zephyrvox_wire::{
    ApiEnvelope, COMMAND_ID, ETAG, EnvelopeError, GEID, PARENT_ETAG, STATE_CURSOR, STREAM_EPOCH,
    SYNC_REQUIRED,
};

/// State and concurrency metadata returned with an HTTP response.
///
/// Missing headers remain `None`; malformed present headers are rejected by
/// the endpoint decoder instead of being silently ignored.  `sync_required`
/// is advisory state metadata and does not itself mutate the client's snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseHeaders {
    /// Resource ETag.
    pub etag: Option<Etag>,
    /// Parent resource ETag.
    pub parent_etag: Option<Etag>,
    /// Durable command identifier.
    pub command_id: Option<String>,
    /// Authenticated state cursor.
    pub state_cursor: Option<Cursor>,
    /// State stream epoch.
    pub stream_epoch: Option<StreamEpoch>,
    /// Highest committed state GEID.
    pub geid: Option<Geid>,
    /// Whether the server requires a replacement snapshot.
    pub sync_required: bool,
}

/// A successful response together with its HTTP and state metadata.
///
/// The typed `data` value is available only after the server's JSON envelope,
/// status, and state-header grammar have all been validated.  A `204` response
/// is represented by endpoint methods returning `ApiResponse<()>` with an empty
/// body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiResponse<T> {
    /// Decoded response data.
    pub data: T,
    /// HTTP status code.
    pub status: u16,
    /// Parsed response headers.
    pub headers: ResponseHeaders,
}

pub(crate) fn decode_json<T: DeserializeOwned>(
    response: HttpResponse,
    endpoint: &str,
) -> Result<ApiResponse<T>, HttpError> {
    let status = response.status();
    if status == 204 {
        return Err(HttpError::UnexpectedNoContent {
            endpoint: endpoint.to_owned(),
        });
    }
    let headers = parse_headers(&response, endpoint)?;
    let envelope: ApiEnvelope<Value> = serde_json::from_slice(response.body())
        .map_err(|error| HttpError::Json(error.to_string()))?;
    if !(200..300).contains(&status) && envelope.code == 0 {
        return Err(HttpError::HttpStatus {
            status,
            endpoint: endpoint.to_owned(),
        });
    }

    match envelope.into_typed::<T>() {
        Ok(data) => Ok(ApiResponse {
            data,
            status,
            headers,
        }),
        Err(error) => Err(map_envelope_error(error, status, endpoint)),
    }
}

pub(crate) fn decode_no_content(
    response: HttpResponse,
    endpoint: &str,
) -> Result<ApiResponse<()>, HttpError> {
    if response.status() == 204 {
        if !response.body().is_empty() {
            return Err(HttpError::UnexpectedContent {
                endpoint: endpoint.to_owned(),
            });
        }
        return Ok(ApiResponse {
            data: (),
            status: 204,
            headers: parse_headers(&response, endpoint)?,
        });
    }
    if response.status() >= 200 && response.status() < 300 {
        return Err(HttpError::ExpectedNoContent {
            endpoint: endpoint.to_owned(),
        });
    }
    match decode_json::<Value>(response, endpoint) {
        Ok(_) => Err(HttpError::ExpectedNoContent {
            endpoint: endpoint.to_owned(),
        }),
        Err(error) => Err(error),
    }
}

fn map_envelope_error(error: EnvelopeError, status: u16, endpoint: &str) -> HttpError {
    match error {
        EnvelopeError::Api(api) => {
            let api = api.with_http_context(
                status,
                endpoint,
                status == 429 || (500..600).contains(&status),
            );
            if status == 412 {
                HttpError::PreconditionFailed(api)
            } else {
                HttpError::Api(api)
            }
        }
        other => HttpError::Envelope(other),
    }
}

fn parse_headers(response: &HttpResponse, endpoint: &str) -> Result<ResponseHeaders, HttpError> {
    Ok(ResponseHeaders {
        etag: parse_optional_etag(response.header(ETAG), ETAG, endpoint)?,
        parent_etag: parse_optional_etag(response.header(PARENT_ETAG), PARENT_ETAG, endpoint)?,
        command_id: response.header(COMMAND_ID).map(str::to_owned),
        state_cursor: parse_optional_cursor(response.header(STATE_CURSOR), endpoint)?,
        stream_epoch: parse_optional_epoch(response.header(STREAM_EPOCH), endpoint)?,
        geid: parse_optional_geid(response.header(GEID), endpoint)?,
        sync_required: parse_sync_required(response.header(SYNC_REQUIRED), endpoint)?,
    })
}

fn parse_optional_etag(
    value: Option<&str>,
    header: &str,
    _endpoint: &str,
) -> Result<Option<Etag>, HttpError> {
    value
        .map(|value| {
            Etag::new(value.to_owned())
                .map_err(|error| HttpError::InvalidHeader(format!("{header}: {error}")))
        })
        .transpose()
}

fn parse_optional_cursor(value: Option<&str>, endpoint: &str) -> Result<Option<Cursor>, HttpError> {
    value
        .map(|value| {
            Cursor::new(value.to_owned())
                .map_err(|error| invalid_response_header(endpoint, STATE_CURSOR, error.to_string()))
        })
        .transpose()
}

fn parse_optional_epoch(
    value: Option<&str>,
    endpoint: &str,
) -> Result<Option<StreamEpoch>, HttpError> {
    value
        .map(|value| {
            StreamEpoch::parse_hex(value)
                .map_err(|error| invalid_response_header(endpoint, STREAM_EPOCH, error.to_string()))
        })
        .transpose()
}

fn parse_optional_geid(value: Option<&str>, endpoint: &str) -> Result<Option<Geid>, HttpError> {
    value
        .map(|value| {
            Geid::from_str(value)
                .map_err(|error| invalid_response_header(endpoint, GEID, error.to_string()))
        })
        .transpose()
}

fn parse_sync_required(value: Option<&str>, endpoint: &str) -> Result<bool, HttpError> {
    match value {
        None => Ok(false),
        Some("true") | Some("TRUE") | Some("True") => Ok(true),
        Some("false") | Some("FALSE") | Some("False") => Ok(false),
        Some(value) => Err(invalid_response_header(
            endpoint,
            SYNC_REQUIRED,
            format!("unsupported boolean value {value}"),
        )),
    }
}

fn invalid_response_header(endpoint: &str, header: &str, reason: String) -> HttpError {
    HttpError::InvalidHeader(format!("{endpoint}: {header}: {reason}"))
}
