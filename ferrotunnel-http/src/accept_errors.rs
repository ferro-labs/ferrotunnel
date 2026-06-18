use std::io;

/// File descriptor exhaustion is transient for accept loops: dropping other
/// connections or raising limits can make the listener usable again.
#[cfg(unix)]
use libc::{EMFILE, ENFILE};

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
    use super::is_transient_accept_error;
    use std::io;

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
