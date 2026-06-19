//! Backoff for server accept loops hitting persistent transient errors.
//!
//! On a persistent `accept` failure (e.g. the TLS acceptor rejecting every
//! connection, or `EMFILE`/`ENFILE` while file descriptors stay exhausted) an
//! accept loop without a delay busy-spins at 100% CPU and floods the logs. This
//! helper applies an exponential, jittered backoff (capped at 1s) and
//! rate-limits the warning so a persistent error does not flood the logs. The
//! backoff resets after a successful accept.
//!
//! This mirrors the HTTP ingress helper in
//! `ferrotunnel-http/src/accept_errors.rs`; it is duplicated here so that
//! `ferrotunnel-core` does not depend on `ferrotunnel-http`.

use crate::reconnect::{Backoff, BackoffConfig};
use std::io;
use std::time::Duration;

/// File descriptor exhaustion is transient for accept loops: dropping other
/// connections or raising limits can make the listener usable again.
#[cfg(unix)]
use libc::{EMFILE, ENFILE};

/// Exponential backoff for an accept loop that keeps hitting transient errors.
pub(crate) struct AcceptBackoff {
    backoff: Backoff,
    consecutive: u64,
}

impl Default for AcceptBackoff {
    fn default() -> Self {
        Self {
            // Millisecond-scale, capped at 1s: long enough to stop pegging a
            // core, short enough that recovery (freed fds) is picked up fast.
            backoff: Backoff::new(BackoffConfig {
                base: Duration::from_millis(5),
                max: Duration::from_secs(1),
                factor: 2.0,
                jitter: 0.3,
            }),
            consecutive: 0,
        }
    }
}

impl AcceptBackoff {
    /// Reset after a successful accept so the next error starts from the base
    /// delay again.
    pub(crate) fn reset(&mut self) {
        self.backoff.reset();
        self.consecutive = 0;
    }

    /// Record a transient accept failure. Returns the delay to sleep before the
    /// next `accept()` and whether this failure should be logged (logging is
    /// rate-limited so a persistent error does not flood the logs).
    pub(crate) fn record_failure(&mut self) -> (Duration, bool) {
        self.consecutive += 1;
        let should_log = self.consecutive == 1 || self.consecutive.is_multiple_of(50);
        (self.backoff.next_delay(), should_log)
    }
}

/// Whether an accept error is transient and worth retrying after a backoff.
pub(crate) fn is_transient_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) || is_fd_exhaustion(error)
}

#[cfg(unix)]
fn is_fd_exhaustion(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(EMFILE | ENFILE))
}

#[cfg(not(unix))]
fn is_fd_exhaustion(_error: &io::Error) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::{is_transient_accept_error, AcceptBackoff};
    use std::io;

    #[test]
    fn backoff_grows_then_resets() {
        let mut backoff = AcceptBackoff::default();

        let (first, log_first) = backoff.record_failure();
        let (second, _) = backoff.record_failure();
        // Delay escalates while the error persists.
        assert!(second >= first);
        // The first consecutive failure is always logged.
        assert!(log_first);

        backoff.reset();
        // After a successful accept the next failure starts logging again.
        let (_, log_after_reset) = backoff.record_failure();
        assert!(log_after_reset);
    }

    #[test]
    fn backoff_rate_limits_logging() {
        let mut backoff = AcceptBackoff::default();
        let logged = (0..120).filter(|_| backoff.record_failure().1).count();
        // 1st, 50th, 100th -> 3 log lines out of 120 failures.
        assert_eq!(logged, 3);
    }

    #[test]
    fn classifies_retryable_accept_errors() {
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
        ] {
            assert!(is_transient_accept_error(&io::Error::new(kind, "accept")));
        }
    }

    #[test]
    fn classifies_fatal_accept_errors() {
        for kind in [
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::PermissionDenied,
        ] {
            assert!(!is_transient_accept_error(&io::Error::new(kind, "accept")));
        }
    }

    #[cfg(unix)]
    #[test]
    fn classifies_file_descriptor_exhaustion_as_retryable() {
        assert!(is_transient_accept_error(&io::Error::from_raw_os_error(
            libc::EMFILE
        )));
        assert!(is_transient_accept_error(&io::Error::from_raw_os_error(
            libc::ENFILE
        )));
    }
}
