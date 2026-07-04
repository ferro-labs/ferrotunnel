//! Exponential backoff reconnection logic

use rand::Rng;
use std::time::Duration;

/// Backoff configuration
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay
    pub base: Duration,
    /// Maximum delay
    pub max: Duration,
    /// Multiplier for each attempt
    pub factor: f64,
    /// Jitter factor (0.0 - 1.0)
    pub jitter: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_mins(1),
            factor: 2.0,
            jitter: 0.3,
        }
    }
}

/// Exponential backoff calculator
#[derive(Debug, Clone)]
pub struct Backoff {
    config: BackoffConfig,
    attempt: u32,
}

impl Backoff {
    /// Create a new backoff calculator
    #[must_use]
    pub fn new(config: BackoffConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Get the next delay and increment attempt counter
    #[must_use]
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.calculate_delay();
        self.attempt = self.attempt.saturating_add(1);
        delay
    }

    /// Calculate current delay without incrementing
    #[must_use]
    pub fn current_delay(&self) -> Duration {
        self.calculate_delay()
    }

    /// Reset the backoff (call after successful connection)
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Get current attempt number
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempt
    }

    fn calculate_delay(&self) -> Duration {
        // Calculate base delay with exponential growth (cap attempt to avoid i32 wrap)
        let base_secs = self.config.base.as_secs_f64();
        let attempt: i32 = self
            .attempt
            .min(i32::MAX as u32)
            .try_into()
            .unwrap_or(i32::MAX);
        let exp_delay = base_secs * self.config.factor.powi(attempt);

        // Apply jitter
        let jitter_range = exp_delay * self.config.jitter;
        let jitter = rand::rng().random_range(-jitter_range..=jitter_range);
        let delay_with_jitter = (exp_delay + jitter).max(0.0);

        // Clamp to max
        let final_secs = delay_with_jitter.min(self.config.max.as_secs_f64());

        Duration::from_secs_f64(final_secs)
    }
}

/// Reconnection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectState {
    /// Initial connection attempt
    Connecting,
    /// Connected and running
    Connected,
    /// Waiting before reconnection
    Backoff,
    /// Reconnecting after failure
    Reconnecting,
    /// Permanently failed (max attempts or shutdown)
    Failed,
}

/// Reconnection manager
#[derive(Debug)]
pub struct ReconnectManager {
    backoff: Backoff,
    state: ReconnectState,
    max_attempts: Option<u32>,
}

impl ReconnectManager {
    /// Create a new reconnection manager
    #[must_use]
    pub fn new(config: BackoffConfig, max_attempts: Option<u32>) -> Self {
        Self {
            backoff: Backoff::new(config),
            state: ReconnectState::Connecting,
            max_attempts,
        }
    }

    /// Mark connection as successful
    pub fn on_connected(&mut self) {
        self.backoff.reset();
        self.state = ReconnectState::Connected;
    }

    /// Handle connection failure
    /// Returns the delay before next attempt, or None if should stop
    pub fn on_disconnected(&mut self) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if self.backoff.attempts() >= max {
                self.state = ReconnectState::Failed;
                return None;
            }
        }

        self.state = ReconnectState::Backoff;
        let delay = self.backoff.next_delay();
        Some(delay)
    }

    /// Mark as reconnecting (after backoff wait)
    pub fn start_reconnect(&mut self) {
        self.state = ReconnectState::Reconnecting;
    }

    /// Get current state
    #[must_use]
    pub fn state(&self) -> ReconnectState {
        self.state
    }

    /// Check if should continue trying
    #[must_use]
    pub fn should_retry(&self) -> bool {
        self.state != ReconnectState::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_growth() {
        let config = BackoffConfig {
            base: Duration::from_secs(1),
            max: Duration::from_mins(1),
            factor: 2.0,
            jitter: 0.0, // No jitter for predictable test
        };
        let mut backoff = Backoff::new(config);

        // First delay should be ~1s
        let d1 = backoff.next_delay();
        assert!(d1.as_secs_f64() >= 0.9 && d1.as_secs_f64() <= 1.1);

        // Second should be ~2s
        let d2 = backoff.next_delay();
        assert!(d2.as_secs_f64() >= 1.9 && d2.as_secs_f64() <= 2.1);

        // Third should be ~4s
        let d3 = backoff.next_delay();
        assert!(d3.as_secs_f64() >= 3.9 && d3.as_secs_f64() <= 4.1);
    }

    #[test]
    fn test_backoff_max_cap() {
        let config = BackoffConfig {
            base: Duration::from_secs(10),
            max: Duration::from_secs(30),
            factor: 2.0,
            jitter: 0.0,
        };
        let mut backoff = Backoff::new(config);

        let _ = backoff.next_delay(); // 10
        let _ = backoff.next_delay(); // 20
        let d3 = backoff.next_delay(); // 40 -> capped to 30

        assert!(d3.as_secs() <= 30);
    }

    #[test]
    fn test_backoff_reset() {
        let config = BackoffConfig::default();
        let mut backoff = Backoff::new(config);

        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        assert_eq!(backoff.attempts(), 2);

        backoff.reset();
        assert_eq!(backoff.attempts(), 0);
    }

    #[test]
    fn test_reconnect_manager() {
        let config = BackoffConfig::default();
        let mut manager = ReconnectManager::new(config, Some(2));

        assert!(manager.should_retry());

        // Simulate failures (max_attempts = 2)
        let _ = manager.on_disconnected(); // attempts 0 < 2, increments to 1
        assert!(manager.should_retry());

        let _ = manager.on_disconnected(); // attempts 1 < 2, increments to 2
        assert!(manager.should_retry());

        let _ = manager.on_disconnected(); // attempts 2 >= 2, fails
        assert!(!manager.should_retry()); // Max reached
    }
}
