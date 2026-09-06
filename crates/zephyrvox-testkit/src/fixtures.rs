use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

use serde::de::DeserializeOwned;
use thiserror::Error;

/// A filesystem-backed loader for checked-in protocol fixtures.
///
/// The loader accepts only relative paths composed of normal components.  It
/// is intended for tests and interoperability harnesses, and never permits a
/// fixture name to escape the configured root.
#[derive(Debug, Clone)]
pub struct FixtureLoader {
    root: PathBuf,
}

impl FixtureLoader {
    /// Creates a loader rooted at a fixture directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the configured fixture root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Reads one fixture as raw bytes.
    pub fn read_bytes(&self, name: impl AsRef<Path>) -> Result<Vec<u8>, FixtureError> {
        let path = self.resolve(name.as_ref())?;
        fs::read(path).map_err(FixtureError::Io)
    }

    /// Reads one fixture as UTF-8 text.
    pub fn read_string(&self, name: impl AsRef<Path>) -> Result<String, FixtureError> {
        let bytes = self.read_bytes(name)?;
        String::from_utf8(bytes).map_err(FixtureError::Utf8)
    }

    /// Reads and deserializes one JSON fixture.
    pub fn read_json<T: DeserializeOwned>(
        &self,
        name: impl AsRef<Path>,
    ) -> Result<T, FixtureError> {
        let text = self.read_string(name)?;
        serde_json::from_str(&text).map_err(FixtureError::Json)
    }

    fn resolve(&self, name: &Path) -> Result<PathBuf, FixtureError> {
        if name.as_os_str().is_empty() || name.is_absolute() {
            return Err(FixtureError::InvalidName(name.to_owned()));
        }
        if name.components().any(|component| {
            matches!(
                component,
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        }) {
            return Err(FixtureError::InvalidName(name.to_owned()));
        }
        let root = fs::canonicalize(&self.root).map_err(FixtureError::Io)?;
        let path = fs::canonicalize(root.join(name)).map_err(FixtureError::Io)?;
        if !path.starts_with(&root) {
            return Err(FixtureError::InvalidName(name.to_owned()));
        }
        Ok(path)
    }
}

/// Errors raised while loading a checked-in fixture.
#[derive(Debug, Error)]
pub enum FixtureError {
    /// The requested fixture name was not a safe relative path.
    #[error("invalid fixture name: {0}")]
    InvalidName(PathBuf),
    /// The fixture could not be read.
    #[error("could not read fixture: {0}")]
    Io(#[source] io::Error),
    /// The fixture was not valid UTF-8.
    #[error("fixture is not valid UTF-8: {0}")]
    Utf8(#[source] std::string::FromUtf8Error),
    /// The fixture was not valid JSON for the requested type.
    #[error("could not decode JSON fixture: {0}")]
    Json(#[source] serde_json::Error),
}
