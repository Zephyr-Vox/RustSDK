//! Terminal lifecycle operations for the high-level client.

use std::sync::atomic::Ordering;

use crate::{Client, SdkError};

impl Client {
    /// Stops voice and control tasks and makes every later facade operation
    /// return [`SdkError::Closed`].
    ///
    /// Shutdown is idempotent across cloned `Client` handles. It stops UDP
    /// before closing the control socket, aborts the facade event relays, and
    /// clears cached voice keys. It does not clear persisted HTTP credentials.
    ///
    /// # Errors
    ///
    /// Returns the first terminal voice or realtime teardown error while still
    /// attempting every remaining cleanup step.
    pub async fn shutdown(&self) -> Result<(), SdkError> {
        let _shutdown_guard = self.inner.shutdown_gate.lock().await;
        if self.inner.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let mut first_error = None;
        {
            let _voice_gate = self.inner.voice_gate.lock().await;
            if let Some(session) = self.inner.voice.lock().await.take()
                && let Err(error) = session.close_transport().await
            {
                first_error = Some(error);
            }
        }
        if let Some(control) = self.inner.control.lock().await.take()
            && let Err(error) = control.close().await.map_err(SdkError::from)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        self.inner.key_cache.clear();
        if let Ok(mut intent) = self.inner.voice_intent.lock() {
            *intent = None;
        }
        if let Ok(mut state) = self.inner.state.write() {
            *state = None;
        }

        let tasks = std::mem::take(&mut *self.inner.tasks.lock().await);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Reports the configured automatic voice-rejoin preference.
    ///
    /// Returns whether ordinary reconnects may replay the last voice join.
    /// Explicit leave, authority loss, and authentication revocation always
    /// clear the remembered intent.
    pub fn auto_rejoin_voice(&self) -> bool {
        self.inner.auto_rejoin_voice
    }

    /// Rejects operations after the client has entered terminal shutdown.
    pub(crate) fn ensure_open(&self) -> Result<(), SdkError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            Err(SdkError::Closed)
        } else {
            Ok(())
        }
    }
}
