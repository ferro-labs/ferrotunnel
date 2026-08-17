//! Configuration types for `FerroTunnel` client and server.
//!
//! These types provide type-safe configuration for embedding `FerroTunnel`
//! in your applications.
//!
//! Config fields are private; construct these types through
//! [`ClientBuilder`](crate::ClientBuilder) / [`ServerBuilder`](crate::ServerBuilder)
//! and read them back through the getters. This keeps the authentication token
//! out of derived `Debug` output and lets new fields be added without breaking
//! callers.

use ferrotunnel_common::{
    LimitsConfig, RateLimitConfig, Result, TunnelError, DEFAULT_HTTP_PORT, DEFAULT_LOCAL_ADDR,
    DEFAULT_TUNNEL_PORT,
};
use ferrotunnel_core::{validate_limits, validate_rate_limits};
use std::fmt;
use std::net::SocketAddr;
#[cfg(feature = "http3")]
use std::path::Path;
#[cfg(feature = "http3")]
use std::path::PathBuf;
use std::time::Duration;

/// Placeholder shown in place of the authentication token in `Debug` output.
const REDACTED: &str = "<redacted>";

/// Configuration for the tunnel client.
///
/// Use [`ClientBuilder`](crate::ClientBuilder) for ergonomic construction, then
/// read values back through the getters. The `Debug` implementation redacts the
/// authentication token.
#[derive(Clone)]
pub struct ClientConfig {
    /// Server address to connect to (host:port)
    pub(crate) server_addr: String,

    /// Authentication token
    pub(crate) token: String,

    /// Local address to forward traffic to
    pub(crate) local_addr: String,

    /// Tunnel ID used for HTTP routing (matched against the Host header)
    pub(crate) tunnel_id: Option<String>,

    /// Enable automatic reconnection on disconnect
    pub(crate) auto_reconnect: bool,

    /// Delay between reconnection attempts
    pub(crate) reconnect_delay: Duration,

    /// Maximum time to wait for the initial connection in `start()`. `None` waits indefinitely. Default 30s.
    pub(crate) startup_timeout: Option<Duration>,

    /// Resource limits for protocol framing and connection handling.
    pub(crate) limits: LimitsConfig,
}

impl ClientConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.server_addr.is_empty() {
            return Err(TunnelError::Config("server_addr is required".into()));
        }
        if self.token.is_empty() {
            return Err(TunnelError::Config("token is required".into()));
        }
        if self.local_addr.is_empty() {
            return Err(TunnelError::Config("local_addr is required".into()));
        }
        validate_limits(&self.limits)?;
        Ok(())
    }

    /// Server address the client connects to (`host:port`).
    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    /// Authentication token presented to the server.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Local address traffic is forwarded to.
    pub fn local_addr(&self) -> &str {
        &self.local_addr
    }

    /// Tunnel ID used for HTTP routing, if set.
    pub fn tunnel_id(&self) -> Option<&str> {
        self.tunnel_id.as_deref()
    }

    /// Whether automatic reconnection is enabled.
    pub fn auto_reconnect(&self) -> bool {
        self.auto_reconnect
    }

    /// Delay between reconnection attempts.
    pub fn reconnect_delay(&self) -> Duration {
        self.reconnect_delay
    }

    /// Maximum time `start()` waits for the initial connection. `None` waits indefinitely.
    pub fn startup_timeout(&self) -> Option<Duration> {
        self.startup_timeout
    }

    /// Resource limits for protocol framing and connection handling.
    pub fn limits(&self) -> &LimitsConfig {
        &self.limits
    }
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("server_addr", &self.server_addr)
            .field("token", &REDACTED)
            .field("local_addr", &self.local_addr)
            .field("tunnel_id", &self.tunnel_id)
            .field("auto_reconnect", &self.auto_reconnect)
            .field("reconnect_delay", &self.reconnect_delay)
            .field("startup_timeout", &self.startup_timeout)
            .field("limits", &self.limits)
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: String::new(),
            token: String::new(),
            local_addr: DEFAULT_LOCAL_ADDR.to_string(),
            tunnel_id: None,
            auto_reconnect: true,
            reconnect_delay: Duration::from_secs(5),
            startup_timeout: Some(Duration::from_secs(30)),
            limits: LimitsConfig::default(),
        }
    }
}

/// Configuration for the tunnel server.
///
/// Use [`ServerBuilder`](crate::ServerBuilder) for ergonomic construction, then
/// read values back through the getters. The `Debug` implementation redacts the
/// authentication token.
#[derive(Clone)]
pub struct ServerConfig {
    /// Address to bind the tunnel control plane
    pub(crate) bind_addr: SocketAddr,

    /// Address to bind the HTTP ingress
    pub(crate) http_bind_addr: SocketAddr,

    /// Optional address to bind HTTP/3 ingress (UDP)
    #[cfg(feature = "http3")]
    pub(crate) http3_bind_addr: Option<SocketAddr>,

    /// Path to certificate file for HTTP/3 ingress
    #[cfg(feature = "http3")]
    pub(crate) http3_cert_path: Option<PathBuf>,

    /// Path to private key file for HTTP/3 ingress
    #[cfg(feature = "http3")]
    pub(crate) http3_key_path: Option<PathBuf>,

    /// Authentication token (clients must provide this)
    pub(crate) token: String,

    /// Resource limits for protocol framing and connection handling.
    pub(crate) limits: LimitsConfig,

    /// Per-session rate limits enforced by the tunnel server.
    pub(crate) rate_limits: RateLimitConfig,

    /// Upstream response timeout for the HTTP and HTTP/3 ingress.
    pub(crate) http_response_timeout: Duration,
}

impl ServerConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.token.is_empty() {
            return Err(TunnelError::Config("token is required".into()));
        }
        validate_limits(&self.limits)?;
        if self.http_response_timeout.is_zero() {
            return Err(TunnelError::Config(
                "http_response_timeout must be greater than zero".into(),
            ));
        }
        validate_rate_limits(&self.rate_limits)?;
        #[cfg(feature = "http3")]
        if self.http3_bind_addr.is_some()
            && (self.http3_cert_path.is_none() || self.http3_key_path.is_none())
        {
            return Err(TunnelError::Config(
                "HTTP/3 ingress requires certificate and key paths".into(),
            ));
        }
        Ok(())
    }

    /// Address the tunnel control plane binds to.
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Address the HTTP ingress binds to.
    pub fn http_bind_addr(&self) -> SocketAddr {
        self.http_bind_addr
    }

    /// Address the HTTP/3 ingress binds to, if configured.
    #[cfg(feature = "http3")]
    pub fn http3_bind_addr(&self) -> Option<SocketAddr> {
        self.http3_bind_addr
    }

    /// Certificate path for the HTTP/3 ingress, if configured.
    #[cfg(feature = "http3")]
    pub fn http3_cert_path(&self) -> Option<&Path> {
        self.http3_cert_path.as_deref()
    }

    /// Private-key path for the HTTP/3 ingress, if configured.
    #[cfg(feature = "http3")]
    pub fn http3_key_path(&self) -> Option<&Path> {
        self.http3_key_path.as_deref()
    }

    /// Authentication token clients must present.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Resource limits for protocol framing and connection handling.
    pub fn limits(&self) -> &LimitsConfig {
        &self.limits
    }

    /// Per-session rate limits enforced by the tunnel server.
    pub fn rate_limits(&self) -> &RateLimitConfig {
        &self.rate_limits
    }

    /// Upstream response timeout for the HTTP and HTTP/3 ingress.
    pub fn http_response_timeout(&self) -> Duration {
        self.http_response_timeout
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ServerConfig");
        debug
            .field("bind_addr", &self.bind_addr)
            .field("http_bind_addr", &self.http_bind_addr);
        #[cfg(feature = "http3")]
        debug
            .field("http3_bind_addr", &self.http3_bind_addr)
            .field("http3_cert_path", &self.http3_cert_path)
            .field("http3_key_path", &self.http3_key_path);
        debug
            .field("token", &REDACTED)
            .field("limits", &self.limits)
            .field("rate_limits", &self.rate_limits)
            .field("http_response_timeout", &self.http_response_timeout)
            .finish()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: ([0, 0, 0, 0], DEFAULT_TUNNEL_PORT).into(),
            http_bind_addr: ([0, 0, 0, 0], DEFAULT_HTTP_PORT).into(),
            #[cfg(feature = "http3")]
            http3_bind_addr: None,
            #[cfg(feature = "http3")]
            http3_cert_path: None,
            #[cfg(feature = "http3")]
            http3_key_path: None,
            token: String::new(),
            limits: LimitsConfig::default(),
            rate_limits: RateLimitConfig::default(),
            http_response_timeout: Duration::from_mins(1),
        }
    }
}

/// Information about an established tunnel connection.
///
/// Returned by `Client::start()`. Read the fields through the getters.
#[derive(Debug, Clone)]
pub struct TunnelInfo {
    /// The session ID assigned by the server.
    ///
    /// Currently a client-generated placeholder until the core library exposes
    /// the server-assigned session ID.
    pub(crate) session_id: Option<uuid::Uuid>,

    /// Reserved for the public URL where the tunnel is reachable.
    ///
    /// Not yet populated: `public_url()` currently always returns `None`. It is
    /// kept so callers can adopt the accessor before the value is wired up.
    pub(crate) public_url: Option<String>,
}

impl TunnelInfo {
    /// The session ID for the established tunnel, if assigned.
    pub fn session_id(&self) -> Option<uuid::Uuid> {
        self.session_id
    }

    /// The public URL where the tunnel is reachable.
    ///
    /// Reserved for future use; currently always `None`.
    pub fn public_url(&self) -> Option<&str> {
        self.public_url.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        assert!(config.server_addr.is_empty());
        assert!(config.token.is_empty());
        assert_eq!(config.local_addr, "127.0.0.1:8080");
        assert!(config.auto_reconnect);
        assert_eq!(config.reconnect_delay, Duration::from_secs(5));
        assert_eq!(config.startup_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.limits.max_frame_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn test_client_config_validate_success() {
        let config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: "secret-token".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_client_config_validate_rejects_zero_frame_limit() {
        let mut config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: "secret-token".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            ..Default::default()
        };
        config.limits.max_frame_bytes = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"));
    }

    #[test]
    fn test_client_config_validate_rejects_oversized_frame_limit() {
        let mut config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: "secret-token".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            ..Default::default()
        };
        // Just above the protocol frame ceiling (16 MiB) is now rejected.
        config.limits.max_frame_bytes = 17 * 1024 * 1024;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"));
    }

    #[test]
    fn test_client_config_validate_missing_server_addr() {
        let config = ClientConfig {
            server_addr: String::new(),
            token: "secret".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("server_addr"));
    }

    #[test]
    fn test_client_config_validate_missing_token() {
        let config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: String::new(),
            local_addr: "127.0.0.1:8080".to_string(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("token"));
    }

    #[test]
    fn test_client_config_validate_missing_local_addr() {
        let config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: "secret".to_string(),
            local_addr: String::new(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("local_addr"));
    }

    #[test]
    fn test_client_config_getters() {
        let config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: "secret".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            tunnel_id: Some("my-tunnel".to_string()),
            ..Default::default()
        };
        assert_eq!(config.server_addr(), "localhost:7835");
        assert_eq!(config.token(), "secret");
        assert_eq!(config.local_addr(), "127.0.0.1:8080");
        assert_eq!(config.tunnel_id(), Some("my-tunnel"));
        assert!(config.auto_reconnect());
        assert_eq!(config.reconnect_delay(), Duration::from_secs(5));
        assert_eq!(config.startup_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(config.limits().max_frame_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn test_client_config_debug_redacts_token() {
        let config = ClientConfig {
            server_addr: "localhost:7835".to_string(),
            token: "super-secret-token".to_string(),
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.bind_addr, SocketAddr::from(([0, 0, 0, 0], 7835)));
        assert_eq!(
            config.http_bind_addr,
            SocketAddr::from(([0, 0, 0, 0], 8080))
        );
        assert!(config.token.is_empty());
        assert_eq!(config.limits.max_frame_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn test_server_config_validate_success() {
        let config = ServerConfig {
            token: "secret-token".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_server_config_validate_rejects_zero_frame_limit() {
        let mut config = ServerConfig {
            token: "secret-token".to_string(),
            ..Default::default()
        };
        config.limits.max_frame_bytes = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"));
    }

    #[test]
    fn test_server_config_validate_rejects_oversized_frame_limit() {
        let mut config = ServerConfig {
            token: "secret-token".to_string(),
            ..Default::default()
        };
        // Just above the protocol frame ceiling (16 MiB) is now rejected.
        config.limits.max_frame_bytes = 17 * 1024 * 1024;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"));
    }

    #[test]
    fn test_server_config_validate_missing_token() {
        let config = ServerConfig::default();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("token"));
    }

    #[test]
    fn test_server_config_getters() {
        let config = ServerConfig {
            token: "secret".to_string(),
            ..Default::default()
        };
        assert_eq!(config.bind_addr(), SocketAddr::from(([0, 0, 0, 0], 7835)));
        assert_eq!(
            config.http_bind_addr(),
            SocketAddr::from(([0, 0, 0, 0], 8080))
        );
        assert_eq!(config.token(), "secret");
        assert_eq!(config.limits().max_frame_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn test_server_config_debug_redacts_token() {
        let config = ServerConfig {
            token: "super-secret-token".to_string(),
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn test_tunnel_info_none_values() {
        let info = TunnelInfo {
            session_id: None,
            public_url: None,
        };
        assert!(info.session_id().is_none());
        assert!(info.public_url().is_none());
    }

    #[test]
    fn test_tunnel_info_with_values() {
        let uuid = uuid::Uuid::new_v4();
        let info = TunnelInfo {
            session_id: Some(uuid),
            public_url: Some("https://tunnel.example.com".to_string()),
        };
        assert_eq!(info.session_id(), Some(uuid));
        assert_eq!(info.public_url(), Some("https://tunnel.example.com"));
    }
}
