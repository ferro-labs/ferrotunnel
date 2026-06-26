//! Configuration types for `FerroTunnel` client and server.
//!
//! These types provide type-safe configuration for embedding `FerroTunnel`
//! in your applications.

use ferrotunnel_common::{
    Result, TunnelError, DEFAULT_HTTP_PORT, DEFAULT_LOCAL_ADDR, DEFAULT_TUNNEL_PORT,
};
use std::net::SocketAddr;
#[cfg(feature = "http3")]
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the tunnel client.
///
/// Use [`ClientBuilder`](crate::ClientBuilder) for ergonomic construction.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server address to connect to (host:port)
    pub server_addr: String,

    /// Authentication token
    pub token: String,

    /// Local address to forward traffic to
    pub local_addr: String,

    /// Tunnel ID used for HTTP routing (matched against the Host header)
    pub tunnel_id: Option<String>,

    /// Enable automatic reconnection on disconnect
    pub auto_reconnect: bool,

    /// Delay between reconnection attempts
    pub reconnect_delay: Duration,

    /// Maximum time to wait for the initial connection in `start()`. `None` waits indefinitely. Default 30s.
    pub startup_timeout: Option<Duration>,
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
        Ok(())
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
        }
    }
}

/// Configuration for the tunnel server.
///
/// Use [`ServerBuilder`](crate::ServerBuilder) for ergonomic construction.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind the tunnel control plane
    pub bind_addr: SocketAddr,

    /// Address to bind the HTTP ingress
    pub http_bind_addr: SocketAddr,

    /// Optional address to bind HTTP/3 ingress (UDP)
    #[cfg(feature = "http3")]
    pub http3_bind_addr: Option<SocketAddr>,

    /// Path to certificate file for HTTP/3 ingress
    #[cfg(feature = "http3")]
    pub http3_cert_path: Option<PathBuf>,

    /// Path to private key file for HTTP/3 ingress
    #[cfg(feature = "http3")]
    pub http3_key_path: Option<PathBuf>,

    /// Authentication token (clients must provide this)
    pub token: String,
}

impl ServerConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.token.is_empty() {
            return Err(TunnelError::Config("token is required".into()));
        }
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
        }
    }
}

/// Information about an established tunnel connection.
#[derive(Debug, Clone)]
pub struct TunnelInfo {
    /// The session ID assigned by the server.
    ///
    /// Note: Currently this is a client-generated placeholder until
    /// the core library exposes the server-assigned session ID.
    pub session_id: Option<uuid::Uuid>,

    /// The public URL where the tunnel is accessible (if applicable)
    pub public_url: Option<String>,
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
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.bind_addr, SocketAddr::from(([0, 0, 0, 0], 7835)));
        assert_eq!(
            config.http_bind_addr,
            SocketAddr::from(([0, 0, 0, 0], 8080))
        );
        assert!(config.token.is_empty());
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
    fn test_server_config_validate_missing_token() {
        let config = ServerConfig::default();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("token"));
    }

    #[test]
    fn test_tunnel_info_none_values() {
        let info = TunnelInfo {
            session_id: None,
            public_url: None,
        };
        assert!(info.session_id.is_none());
        assert!(info.public_url.is_none());
    }

    #[test]
    fn test_tunnel_info_with_values() {
        let uuid = uuid::Uuid::new_v4();
        let info = TunnelInfo {
            session_id: Some(uuid),
            public_url: Some("https://tunnel.example.com".to_string()),
        };
        assert_eq!(info.session_id, Some(uuid));
        assert_eq!(
            info.public_url,
            Some("https://tunnel.example.com".to_string())
        );
    }
}
