//! Configuration types for `FerroTunnel` hardening

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// Path to CA certificate (for client verification or server trust)
    pub ca_cert_path: Option<PathBuf>,
    /// Path to certificate file
    pub cert_path: Option<PathBuf>,
    /// Path to private key file
    pub key_path: Option<PathBuf>,
    /// Server name for SNI (client-side)
    pub server_name: Option<String>,
    /// Require client certificate authentication
    pub client_auth: bool,
    /// Skip certificate verification (insecure, for self-signed certs only).
    ///
    /// Enabling this leaves TLS connections unauthenticated and vulnerable to
    /// man-in-the-middle interception. Client config builders emit a warning
    /// whenever this is used.
    ///
    /// Defaults to `false` so older config files that predate this field
    /// deserialize without error and keep verification enabled.
    #[serde(default)]
    pub skip_verify: bool,
}

/// Resource limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    /// Maximum frame size in bytes (default: 16MB)
    pub max_frame_bytes: u64,
    /// Maximum concurrent sessions per server
    pub max_sessions: usize,
    /// Maximum streams per session
    pub max_streams_per_session: usize,
    /// Maximum in-flight frames per session
    pub max_inflight_frames: usize,
    /// Maximum token length in bytes
    pub max_token_len: usize,
    /// Maximum number of capabilities
    pub max_capabilities: usize,
    /// Maximum capability string length
    pub max_capability_len: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024, // 16MB
            max_sessions: 1000,
            max_streams_per_session: 100,
            max_inflight_frames: 100,
            max_token_len: 256,
            max_capabilities: 32,
            max_capability_len: 64,
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum new streams per second per session
    pub streams_per_sec: u32,
    /// Maximum bytes per second per session
    pub bytes_per_sec: u64,
    /// Burst allowance multiplier applied to the per-second rates.
    ///
    /// The effective burst capacity is `rate * burst_factor`, saturating at
    /// `u32::MAX`; a value of `0` is treated as `1` by the rate limiter. Values
    /// beyond a small single-digit multiple offer no practical benefit.
    pub burst_factor: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            streams_per_sec: 100,
            bytes_per_sec: 10 * 1024 * 1024, // 10MB/s
            burst_factor: 2,
        }
    }
}

/// QUIC transport configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuicConfig {
    /// Enable QUIC transport
    pub enabled: bool,
    /// Path to certificate file (required — QUIC mandates TLS 1.3)
    pub cert_path: Option<PathBuf>,
    /// Path to private key file
    pub key_path: Option<PathBuf>,
    /// Path to CA certificate (for peer verification)
    pub ca_cert_path: Option<PathBuf>,
    /// Server name for SNI (client-side)
    pub server_name: Option<String>,
    /// Request 0-RTT on reconnect; currently falls back to a full handshake
    pub enable_0rtt: bool,
    /// Maximum idle timeout in seconds (default: 30)
    pub max_idle_timeout_secs: Option<u64>,
    /// Keep-alive interval in seconds (default: 10)
    pub keep_alive_interval_secs: Option<u64>,
    /// Skip certificate verification (insecure, for self-signed certs only).
    ///
    /// Enabling this leaves QUIC connections unauthenticated and vulnerable to
    /// man-in-the-middle interception. Client config builders emit a warning
    /// whenever this is used.
    ///
    /// Defaults to `false` so older config files that predate this field
    /// deserialize without error and keep verification enabled.
    #[serde(default)]
    pub skip_verify: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_without_skip_verify_defaults_to_secure() {
        // Simulates a config file written before `skip_verify` existed.
        let json = r#"{ "enabled": true, "client_auth": false }"#;
        let config: TlsConfig =
            serde_json::from_str(json).expect("legacy TLS config must still deserialize");
        assert!(
            !config.skip_verify,
            "missing skip_verify must default to false (verification enabled)"
        );
        assert!(config.enabled);
    }

    #[test]
    fn quic_config_without_skip_verify_defaults_to_secure() {
        let json = r#"{ "enabled": true, "enable_0rtt": false }"#;
        let config: QuicConfig =
            serde_json::from_str(json).expect("legacy QUIC config must still deserialize");
        assert!(
            !config.skip_verify,
            "missing skip_verify must default to false (verification enabled)"
        );
        assert!(config.enabled);
    }

    #[test]
    fn skip_verify_round_trips_when_present() {
        let json = r#"{ "enabled": true, "client_auth": false, "skip_verify": true }"#;
        let config: TlsConfig =
            serde_json::from_str(json).expect("config with skip_verify must deserialize");
        assert!(config.skip_verify);
    }
}
