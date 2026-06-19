//! Rate limiting for tunnel sessions

use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Session rate limiter configuration
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Maximum streams opened per second
    pub streams_per_sec: NonZeroU32,
    /// Maximum bytes per second
    pub bytes_per_sec: NonZeroU32,
    /// Burst multiplier
    pub burst_factor: NonZeroU32,
}

impl Default for RateLimiterConfig {
    #[allow(clippy::expect_used)]
    fn default() -> Self {
        Self {
            streams_per_sec: NonZeroU32::new(100).expect("100 is non-zero"),
            bytes_per_sec: NonZeroU32::new(10 * 1024 * 1024).expect("10MB is non-zero"),
            burst_factor: NonZeroU32::new(2).expect("2 is non-zero"),
        }
    }
}

/// Compute an absolute burst capacity from a per-second `rate` and a `burst_factor`
/// multiplier, saturating at [`NonZeroU32::MAX`] to avoid overflow.
fn scaled_burst(rate: NonZeroU32, burst_factor: NonZeroU32) -> NonZeroU32 {
    NonZeroU32::new(rate.get().saturating_mul(burst_factor.get())).unwrap_or(NonZeroU32::MAX)
}

/// Rate limiter for a single session
#[derive(Clone)]
pub struct SessionRateLimiter {
    /// Limits stream open rate
    stream_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    /// Limits data throughput
    bytes_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
}

impl SessionRateLimiter {
    /// Create a new session rate limiter
    #[must_use]
    pub fn new(config: &RateLimiterConfig) -> Self {
        // `allow_burst` sets the ABSOLUTE burst capacity (in cells), not a multiplier.
        // Because the byte limiter consumes one cell per byte via `check_n`, the burst
        // capacity must scale with the rate (rate * burst_factor); otherwise a tiny
        // burst would reject every real data frame.
        let stream_quota = Quota::per_second(config.streams_per_sec)
            .allow_burst(scaled_burst(config.streams_per_sec, config.burst_factor));
        let bytes_quota = Quota::per_second(config.bytes_per_sec)
            .allow_burst(scaled_burst(config.bytes_per_sec, config.burst_factor));

        Self {
            stream_limiter: Arc::new(RateLimiter::direct(stream_quota)),
            bytes_limiter: Arc::new(RateLimiter::direct(bytes_quota)),
        }
    }

    /// Check if a new stream can be opened
    /// Returns Ok(()) if allowed, Err if rate limited
    pub fn check_stream_open(&self) -> Result<(), RateLimitError> {
        self.stream_limiter
            .check()
            .map_err(|_| RateLimitError::StreamRateLimited)
    }

    /// Check if `bytes` of data can be sent, accounting for the actual payload size.
    ///
    /// Consumes `bytes` tokens from the byte-rate quota via governor's `check_n`
    /// rather than a single token per frame. Empty payloads are always allowed.
    ///
    /// Fails closed: if the payload exceeds the quota's burst capacity (so it could
    /// never conform), or the current rate is exceeded, this returns an error.
    pub fn check_data(&self, bytes: usize) -> Result<(), RateLimitError> {
        // Zero-length frames consume no quota.
        let Some(n) = NonZeroU32::new(u32::try_from(bytes).unwrap_or(u32::MAX)) else {
            return Ok(());
        };

        match self.bytes_limiter.check_n(n) {
            // All requested cells fit within the current rate.
            Ok(Ok(())) => Ok(()),
            // Current rate exceeded; the batch may conform later.
            Ok(Err(_)) => Err(RateLimitError::BytesRateLimited),
            // Payload can never fit the burst capacity: fail closed.
            Err(_insufficient_capacity) => Err(RateLimitError::BytesRateLimited),
        }
    }

    /// Async version - waits until allowed
    pub async fn wait_for_stream_open(&self) {
        self.stream_limiter.until_ready().await;
    }

    /// Async version - waits until data can be sent
    pub async fn wait_for_data(&self, _bytes: usize) {
        self.bytes_limiter.until_ready().await;
    }
}

impl std::fmt::Debug for SessionRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionRateLimiter").finish_non_exhaustive()
    }
}

/// Rate limit errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum RateLimitError {
    #[error("stream open rate limited")]
    StreamRateLimited,
    #[error("data transfer rate limited")]
    BytesRateLimited,
}

impl From<RateLimitError> for ferrotunnel_common::TunnelError {
    fn from(err: RateLimitError) -> Self {
        ferrotunnel_common::TunnelError::ServiceUnavailable(err.to_string())
    }
}

/// Convert from common RateLimitConfig to RateLimiterConfig
impl From<ferrotunnel_common::config::RateLimitConfig> for RateLimiterConfig {
    fn from(config: ferrotunnel_common::config::RateLimitConfig) -> Self {
        Self {
            streams_per_sec: NonZeroU32::new(config.streams_per_sec).unwrap_or(NonZeroU32::MIN),
            bytes_per_sec: NonZeroU32::new(u32::try_from(config.bytes_per_sec).unwrap_or(u32::MAX))
                .unwrap_or(NonZeroU32::MIN),
            burst_factor: NonZeroU32::new(config.burst_factor).unwrap_or(NonZeroU32::MIN),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_creation() {
        let config = RateLimiterConfig::default();
        let limiter = SessionRateLimiter::new(&config);

        // Should allow first stream
        assert!(limiter.check_stream_open().is_ok());
    }

    #[test]
    fn test_burst_allowance() {
        let config = RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(1).unwrap(),
            bytes_per_sec: NonZeroU32::new(1000).unwrap(),
            burst_factor: NonZeroU32::new(5).unwrap(),
        };
        let limiter = SessionRateLimiter::new(&config);

        // Burst should allow multiple quick opens
        for _ in 0..5 {
            assert!(limiter.check_stream_open().is_ok());
        }
        // Should be rate limited after burst
        assert!(limiter.check_stream_open().is_err());
    }

    #[test]
    fn check_data_accounts_for_byte_count() {
        // Capacity = bytes_per_sec * burst_factor = 1000 bytes.
        let config = RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(100).unwrap(),
            bytes_per_sec: NonZeroU32::new(1000).unwrap(),
            burst_factor: NonZeroU32::new(1).unwrap(),
        };
        let limiter = SessionRateLimiter::new(&config);

        // A single 1000-byte payload consumes the entire burst (proves bytes are
        // counted, not a single token per frame).
        assert!(limiter.check_data(1000).is_ok());
        // The next equally-sized payload is rejected because the quota is exhausted.
        assert!(limiter.check_data(1000).is_err());
    }

    #[test]
    fn check_data_allows_empty_payload() {
        let config = RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(1).unwrap(),
            bytes_per_sec: NonZeroU32::new(1).unwrap(),
            burst_factor: NonZeroU32::new(1).unwrap(),
        };
        let limiter = SessionRateLimiter::new(&config);
        // Zero-length frames consume no quota and are always allowed.
        assert!(limiter.check_data(0).is_ok());
    }

    #[test]
    fn check_data_fails_closed_when_payload_exceeds_capacity() {
        // Capacity = 10 bytes; a 100-byte payload can never conform.
        let config = RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(100).unwrap(),
            bytes_per_sec: NonZeroU32::new(10).unwrap(),
            burst_factor: NonZeroU32::new(1).unwrap(),
        };
        let limiter = SessionRateLimiter::new(&config);
        // InsufficientCapacity must fail closed.
        assert!(limiter.check_data(100).is_err());
    }
}
