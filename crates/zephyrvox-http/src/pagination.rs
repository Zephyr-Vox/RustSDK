use serde::{Deserialize, Serialize};

/// One bounded page returned by a CommunityServer list endpoint.
///
/// `limit`, `offset`, and `total` are server-selected signed values because
/// they are wire response metadata.  Endpoint methods accept unsigned query
/// arguments; the server remains authoritative for its maximum page size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    /// Items in this page.
    pub items: Vec<T>,
    /// Effective page size selected by the server.
    pub limit: i64,
    /// Zero-based item offset selected by the server.
    pub offset: i64,
    /// Total number of visible items.
    pub total: i64,
}

pub(crate) fn page_path(resource: &str, limit: u32, offset: u32) -> String {
    query_path(
        resource,
        [("limit", limit.to_string()), ("offset", offset.to_string())],
    )
}

pub(crate) fn query_path<I, K, V>(resource: &str, parameters: I) -> String
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in parameters {
        serializer.append_pair(key.as_ref(), value.as_ref());
    }
    let query = serializer.finish();
    if query.is_empty() {
        resource.to_owned()
    } else {
        format!("{resource}?{query}")
    }
}
