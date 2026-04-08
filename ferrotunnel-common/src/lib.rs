//! Common utilities and types for `FerroTunnel`

pub mod config;
pub mod constants;
pub mod error;

pub use config::{LimitsConfig, QuicConfig, RateLimitConfig, TlsConfig};
pub use constants::{
    DEFAULT_DASHBOARD_PORT, DEFAULT_HTTP_BIND, DEFAULT_HTTP_PORT, DEFAULT_LOCAL_ADDR,
    DEFAULT_METRICS_PORT, DEFAULT_TUNNEL_BIND, DEFAULT_TUNNEL_PORT,
};
pub use error::{Result, TunnelError};
