//! Validation for [`LimitsConfig`] shared by the client, server, and the
//! public builder crate.

use ferrotunnel_common::{LimitsConfig, Result, TunnelError};
use ferrotunnel_protocol::constants::MAX_FRAME_SIZE;

/// Validate resource limits before they are used to build codecs and semaphores.
///
/// Rejects values that would either panic codec construction (`max_frame_bytes`
/// of zero or above the protocol frame ceiling) or create a zero-permit
/// semaphore that blocks all sessions/streams.
///
/// # Errors
///
/// Returns [`TunnelError::Config`] describing the first offending field.
pub fn validate_limits(limits: &LimitsConfig) -> Result<()> {
    if limits.max_frame_bytes == 0 {
        return Err(TunnelError::Config(
            "limits.max_frame_bytes must be greater than zero".into(),
        ));
    }
    if limits.max_frame_bytes > u64::from(MAX_FRAME_SIZE) {
        return Err(TunnelError::Config(format!(
            "limits.max_frame_bytes must not exceed {MAX_FRAME_SIZE} bytes"
        )));
    }
    if limits.max_sessions == 0 {
        return Err(TunnelError::Config(
            "limits.max_sessions must be greater than zero".into(),
        ));
    }
    if limits.max_streams_per_session == 0 {
        return Err(TunnelError::Config(
            "limits.max_streams_per_session must be greater than zero".into(),
        ));
    }
    if limits.max_inflight_frames == 0 {
        return Err(TunnelError::Config(
            "limits.max_inflight_frames must be greater than zero".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_default_limits() {
        assert!(validate_limits(&LimitsConfig::default()).is_ok());
    }

    #[test]
    fn rejects_zero_frame_bytes() {
        let limits = LimitsConfig {
            max_frame_bytes: 0,
            ..Default::default()
        };
        assert!(validate_limits(&limits).is_err());
    }

    #[test]
    fn rejects_frame_bytes_above_ceiling() {
        let limits = LimitsConfig {
            max_frame_bytes: u64::from(MAX_FRAME_SIZE) + 1,
            ..Default::default()
        };
        assert!(validate_limits(&limits).is_err());
    }

    #[test]
    fn rejects_zero_sessions() {
        let limits = LimitsConfig {
            max_sessions: 0,
            ..Default::default()
        };
        assert!(validate_limits(&limits).is_err());
    }

    #[test]
    fn rejects_zero_streams_per_session() {
        let limits = LimitsConfig {
            max_streams_per_session: 0,
            ..Default::default()
        };
        assert!(validate_limits(&limits).is_err());
    }

    #[test]
    fn rejects_zero_inflight_frames() {
        let limits = LimitsConfig {
            max_inflight_frames: 0,
            ..Default::default()
        };
        assert!(validate_limits(&limits).is_err());
    }
}
