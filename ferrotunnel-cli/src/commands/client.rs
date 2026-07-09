//! Client subcommand implementation

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Args;
use ferrotunnel_core::TunnelClient;
use ferrotunnel_http::proxy::LocalProxyService;
use ferrotunnel_http::proxy::ProxyError;
use ferrotunnel_observability::dashboard::models::{DashboardTunnelInfo, TunnelStatus};
use ferrotunnel_observability::{init_basic_observability, init_minimal_logging, shutdown_tracing};
use ferrotunnel_protocol::frame::Protocol;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::middleware::DashboardCaptureLayer;

// Need to match the BoxBody type used in ferrotunnel-http
type BoxBody = http_body_util::combinators::BoxBody<bytes::Bytes, ProxyError>;

trait StreamHandler: Send + Sync {
    fn handle(&self, stream: ferrotunnel_core::stream::VirtualStream);
}

impl<L> StreamHandler for ferrotunnel_http::HttpProxy<L>
where
    L: tower::Layer<LocalProxyService> + Clone + Send + Sync + 'static,
    L::Service: tower::Service<
            hyper::Request<hyper::body::Incoming>,
            Response = hyper::Response<BoxBody>,
            Error = hyper::Error,
        > + Send
        + Clone
        + 'static,
    <L::Service as tower::Service<hyper::Request<hyper::body::Incoming>>>::Future: Send,
{
    fn handle(&self, stream: ferrotunnel_core::stream::VirtualStream) {
        if stream.protocol() == Protocol::GRPC {
            self.handle_grpc_stream(stream);
        } else {
            self.handle_stream(stream);
        }
    }
}

/// Client feature configuration (dashboard, TLS, telemetry; flattened into ClientArgs)
#[derive(Args, Debug)]
pub struct ClientFeatureArgs {
    /// Dashboard config struct
    #[command(flatten)]
    pub dashboard: DashboardConfig,

    /// TLS config struct
    #[command(flatten)]
    pub tls: TlsConfig,

    /// Telemetry config struct
    #[command(flatten)]
    pub telemetry: TelemetryConfig,
}

/// Dashboard configuration for tunnel inspection (flattened into ClientFeatureArgs)
#[derive(Args, Debug)]
pub struct DashboardConfig {
    /// Dashboard port
    #[arg(
        long = "dashboard-port",
        default_value = "4040",
        env = "FERROTUNNEL_DASHBOARD_PORT"
    )]
    pub port: u16,

    /// Dashboard bind address. Defaults to loopback; non-loopback requires --dashboard-allow-non-loopback.
    #[arg(
        long = "dashboard-bind",
        default_value = "127.0.0.1",
        env = "FERROTUNNEL_DASHBOARD_BIND"
    )]
    pub bind: std::net::IpAddr,

    /// Allow dashboard to bind to a non-loopback address.
    #[arg(
        long = "dashboard-allow-non-loopback",
        env = "FERROTUNNEL_DASHBOARD_ALLOW_NON_LOOPBACK"
    )]
    pub allow_non_loopback: bool,

    /// Token required for dashboard API requests.
    #[arg(
        long = "dashboard-auth-token",
        env = "FERROTUNNEL_DASHBOARD_AUTH_TOKEN",
        hide_env_values = true
    )]
    pub auth_token: Option<String>,

    /// Disable dashboard
    #[arg(long = "no-dashboard")]
    pub disabled: bool,
}

/// TLS configuration for secure server connections (flattened into ClientFeatureArgs)
#[derive(Args, Debug)]
pub struct TlsConfig {
    /// Enable TLS for server connection. Requires --tls-ca unless --tls-skip-verify is set.
    #[arg(long = "tls", env = "FERROTUNNEL_TLS")]
    pub enabled: bool,

    /// Skip TLS certificate verification. This is insecure and must be explicit.
    #[arg(long = "tls-skip-verify", env = "FERROTUNNEL_TLS_SKIP_VERIFY")]
    pub skip_verify: bool,
}

/// Telemetry and observability configuration (flattened into ClientFeatureArgs)
#[derive(Args, Debug)]
pub struct TelemetryConfig {
    /// Enable tracing (metrics is separate via --metrics)
    #[arg(long, env = "FERROTUNNEL_OBSERVABILITY")]
    pub observability: bool,

    /// Enable metrics collection
    #[arg(long, env = "FERROTUNNEL_METRICS")]
    pub metrics: bool,
}

#[derive(Args, Debug)]
pub struct ClientArgs {
    /// Server address (host:port)
    #[arg(long, env = "FERROTUNNEL_SERVER")]
    server: String,

    /// Authentication token. If omitted, uses FERROTUNNEL_TOKEN env var, or prompts securely.
    #[arg(long, env = "FERROTUNNEL_TOKEN")]
    token: Option<String>,

    /// Reserved CLI option; set RUST_LOG to configure log filtering
    #[arg(long, default_value = "info", env = "RUST_LOG")]
    log_level: String,

    /// Local service address to forward to (host:port)
    #[arg(long, default_value = "127.0.0.1:8000", env = "FERROTUNNEL_LOCAL_ADDR")]
    local_addr: String,

    /// Tunnel ID for HTTP routing (matched against Host header). Auto-generated if omitted.
    #[arg(long, env = "FERROTUNNEL_TUNNEL_ID")]
    tunnel_id: Option<String>,

    #[command(flatten)]
    pub features: ClientFeatureArgs,

    /// Path to CA certificate for TLS verification. Required with --tls unless --tls-skip-verify is set.
    #[arg(long, env = "FERROTUNNEL_TLS_CA")]
    tls_ca: Option<std::path::PathBuf>,

    /// Server name (SNI) for TLS verification
    #[arg(long, env = "FERROTUNNEL_TLS_SERVER_NAME")]
    tls_server_name: Option<String>,

    /// Path to client certificate file (PEM format) for mutual TLS
    #[arg(long, env = "FERROTUNNEL_TLS_CERT")]
    tls_cert: Option<std::path::PathBuf>,

    /// Path to client private key file (PEM format) for mutual TLS
    #[arg(long, env = "FERROTUNNEL_TLS_KEY")]
    tls_key: Option<std::path::PathBuf>,

    /// Use QUIC transport to connect to server (requires server QUIC support)
    #[cfg(feature = "quic")]
    #[arg(long = "quic", env = "FERROTUNNEL_QUIC")]
    quic: bool,

    /// Request QUIC 0-RTT; currently falls back to a full handshake
    #[cfg(feature = "quic")]
    #[arg(long = "quic-0rtt", env = "FERROTUNNEL_QUIC_0RTT")]
    quic_0rtt: bool,
}

/// Resolve token from args, then env, then secure prompt.
fn resolve_token(args: &ClientArgs) -> Result<String> {
    if let Some(ref t) = args.token {
        return Ok(t.clone());
    }
    if let Ok(t) = std::env::var("FERROTUNNEL_TOKEN") {
        return Ok(t);
    }
    prompt_token()
}

/// Prompt for token on TTY without echoing (secure input).
fn prompt_token() -> Result<String> {
    rpassword::prompt_password("Token: ")
        .context("Could not read token from terminal (is stdin a TTY?). Set FERROTUNNEL_TOKEN or pass --token")
}

#[allow(clippy::too_many_lines)]
pub async fn run(args: ClientArgs) -> Result<()> {
    let enable_tracing = args.features.telemetry.observability;
    let enable_metrics = args.features.telemetry.metrics;

    if enable_tracing || enable_metrics {
        init_basic_observability("ferrotunnel-client", enable_tracing, enable_metrics);
    } else {
        init_minimal_logging();
    }

    info!("Starting FerroTunnel Client v{}", env!("CARGO_PKG_VERSION"));

    let token = resolve_token(&args)?;
    validate_tls_args(&args)?;

    // Determine tunnel ID for routing
    let tunnel_id_string: Option<String> = args.tunnel_id.clone().or_else(|| {
        if args.features.dashboard.disabled {
            None
        } else {
            Some(uuid::Uuid::new_v4().to_string())
        }
    });

    // Parse as UUID for dashboard (if it's a valid UUID)
    let dashboard_tunnel_id: Option<uuid::Uuid> =
        tunnel_id_string.as_ref().and_then(|s| s.parse().ok());

    // Start Dashboard and configure proxy
    let proxy: Arc<dyn StreamHandler> = if let Some(tunnel_id) = dashboard_tunnel_id {
        setup_dashboard(&args, tunnel_id).await?
    } else {
        Arc::new(ferrotunnel_http::HttpProxy::new(args.local_addr.clone()))
    };

    // Determine if QUIC transport should be used
    #[cfg(feature = "quic")]
    let use_quic = args.quic;
    #[cfg(not(feature = "quic"))]
    let use_quic = false;

    // Simple reconnection loop with graceful shutdown
    tokio::select! {
        _ = async {
            loop {
                let mut client = TunnelClient::new(args.server.clone(), token.clone());
                if let Some(ref tid) = tunnel_id_string {
                    client = client.with_tunnel_id(tid.clone());
                }

                let connect_result = if use_quic {
                    #[cfg(feature = "quic")]
                    {
                        info!("Using QUIC transport");
                        let quic_config = setup_quic_config(&args);
                        client = client.with_transport(
                            ferrotunnel_core::transport::TransportConfig::Quic(quic_config),
                        );

                        let local_addr_config = args.local_addr.clone();
                        client
                            .connect_and_run_quic(move |stream| {
                                let local_addr = local_addr_config.clone();
                                async move {
                                    // All QUIC streams forwarded via bidirectional copy
                                    tokio::spawn(async move {
                                        match TcpStream::connect(&local_addr).await {
                                            Ok(mut local_stream) => {
                                                let mut tunnel_stream = stream;
                                                let _ = tokio::io::copy_bidirectional(
                                                    &mut tunnel_stream,
                                                    &mut local_stream,
                                                )
                                                .await;
                                            }
                                            Err(e) => {
                                                error!(
                                                    "Failed to connect to local service {}: {}",
                                                    local_addr, e
                                                );
                                            }
                                        }
                                    });
                                }
                            })
                            .await
                    }
                    #[cfg(not(feature = "quic"))]
                    {
                        unreachable!("QUIC not enabled")
                    }
                } else {
                    client = setup_tls(client, &args);
                    let proxy_ref = proxy.clone();
                    let local_addr_config = args.local_addr.clone();
                    client
                        .connect_and_run(move |stream| {
                            let proxy = proxy_ref.clone();
                            let local_addr = local_addr_config.clone();
                            async move {
                                if stream.protocol() == Protocol::TCP {
                                    tokio::spawn(async move {
                                        match TcpStream::connect(&local_addr).await {
                                            Ok(mut local_stream) => {
                                                let mut tunnel_stream = stream;
                                                let _ = tokio::io::copy_bidirectional(
                                                    &mut tunnel_stream,
                                                    &mut local_stream,
                                                )
                                                .await;
                                            }
                                            Err(e) => {
                                                error!(
                                                    "Failed to connect to local TCP service {}: {}",
                                                    local_addr, e
                                                );
                                            }
                                        }
                                    });
                                } else {
                                    proxy.handle(stream);
                                }
                            }
                        })
                        .await
                };

                match connect_result {
                    Ok(()) => {
                        info!("Client finished normally, exiting.");
                        break;
                    }
                    Err(e) => {
                        error!("Connection lost or failed: {}", e);
                        info!("Reconnecting in 5 seconds...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        } => {}
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, disconnecting...");
        }
    }

    shutdown_tracing();
    Ok(())
}

async fn setup_dashboard(
    args: &ClientArgs,
    tunnel_id: uuid::Uuid,
) -> Result<Arc<dyn StreamHandler>> {
    use ferrotunnel_observability::dashboard::{create_router, DashboardState, EventBroadcaster};
    use tokio::sync::RwLock;

    let dashboard_state = Arc::new(RwLock::new(DashboardState::new(1000)));
    let broadcaster = Arc::new(EventBroadcaster::new(100));
    let dashboard_config = &args.features.dashboard;
    let addr = validate_dashboard_config(dashboard_config)?;
    let auth_token = dashboard_config
        .auth_token
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let generated_auth_token = dashboard_config.auth_token.is_none();
    let app = create_router(
        dashboard_state.clone(),
        broadcaster.clone(),
        Some(auth_token.clone()),
    );

    if generated_auth_token {
        info!(
            "Starting Dashboard at http://{}; open http://{}?token={} to authenticate",
            addr, addr, auth_token
        );
    } else {
        info!(
            "Starting Dashboard at http://{} with API authentication enabled",
            addr
        );
    }
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).await {
                    error!("Dashboard server error: {e}");
                }
            }
            Err(e) => {
                error!("Failed to bind dashboard server to {addr}: {e}");
            }
        }
    });

    // Register the local tunnel in the dashboard (same ID used for server routing)
    {
        let mut state = dashboard_state.write().await;
        let tunnel_info = DashboardTunnelInfo {
            id: tunnel_id,
            subdomain: None,
            public_url: None,
            local_addr: args.local_addr.clone(),
            created_at: Utc::now(),
            status: TunnelStatus::Connected,
        };
        state.add_tunnel(tunnel_info);
        info!("Registered tunnel {} in dashboard", tunnel_id);
    }

    // Initialize Proxy with Middleware
    info!("Traffic inspection enabled");
    let capture_layer = DashboardCaptureLayer {
        state: dashboard_state.clone(),
        broadcaster,
        tunnel_id,
    };

    Ok(Arc::new(
        ferrotunnel_http::HttpProxy::new(args.local_addr.clone()).with_layer(capture_layer),
    ))
}

fn validate_dashboard_config(config: &DashboardConfig) -> Result<std::net::SocketAddr> {
    if matches!(config.auth_token.as_deref(), Some("")) {
        anyhow::bail!("Dashboard auth token must not be empty");
    }

    if !config.bind.is_loopback() {
        if !config.allow_non_loopback {
            anyhow::bail!(
                "Refusing to bind dashboard to non-loopback address {}; pass --dashboard-allow-non-loopback to expose it",
                config.bind
            );
        }

        if config.auth_token.is_none() {
            anyhow::bail!(
                "Dashboard auth token is required when binding to non-loopback addresses"
            );
        }
    }

    Ok(std::net::SocketAddr::new(config.bind, config.port))
}

#[cfg(feature = "quic")]
fn setup_quic_config(args: &ClientArgs) -> ferrotunnel_core::transport::quic::QuicTransportConfig {
    ferrotunnel_core::transport::quic::QuicTransportConfig {
        ca_cert_path: args
            .tls_ca
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        cert_path: args
            .tls_cert
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default(),
        key_path: args
            .tls_key
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default(),
        server_name: args.tls_server_name.clone(),
        skip_verify: args.features.tls.skip_verify,
        enable_0rtt: args.quic_0rtt,
        ..Default::default()
    }
}

fn validate_tls_args(args: &ClientArgs) -> Result<()> {
    if args.features.tls.enabled && !args.features.tls.skip_verify && args.tls_ca.is_none() {
        anyhow::bail!(
            "TLS requires --tls-ca for certificate verification; use --tls-skip-verify only for explicit insecure mode"
        );
    }

    Ok(())
}

fn setup_tls(mut client: TunnelClient, args: &ClientArgs) -> TunnelClient {
    if args.features.tls.enabled {
        if args.features.tls.skip_verify {
            info!("TLS enabled with certificate verification skipped (insecure)");
            client = client.with_tls_skip_verify();
        } else if let Some(ref ca_path) = args.tls_ca {
            info!("TLS enabled with CA: {:?}", ca_path);
            client = client.with_tls_ca(ca_path.clone());
        }

        if let Some(ref server_name) = args.tls_server_name {
            info!("TLS SNI enabled with server name: {}", server_name);
            client = client.with_server_name(server_name.clone());
        }

        if let (Some(ref cert_path), Some(ref key_path)) = (&args.tls_cert, &args.tls_key) {
            info!(
                "Mutual TLS enabled with cert: {:?}, key: {:?}",
                cert_path, key_path
            );
            client = client.with_tls(cert_path.clone(), key_path.clone());
        }
    }
    client
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn client_args(tls_enabled: bool, skip_verify: bool, tls_ca: Option<PathBuf>) -> ClientArgs {
        ClientArgs {
            server: "127.0.0.1:7835".to_string(),
            token: Some("token".to_string()),
            log_level: "info".to_string(),
            local_addr: "127.0.0.1:8000".to_string(),
            tunnel_id: None,
            features: ClientFeatureArgs {
                dashboard: DashboardConfig {
                    port: 4040,
                    bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    allow_non_loopback: false,
                    auth_token: None,
                    disabled: true,
                },
                tls: TlsConfig {
                    enabled: tls_enabled,
                    skip_verify,
                },
                telemetry: TelemetryConfig {
                    observability: false,
                    metrics: false,
                },
            },
            tls_ca,
            tls_server_name: None,
            tls_cert: None,
            tls_key: None,
            #[cfg(feature = "quic")]
            quic: false,
            #[cfg(feature = "quic")]
            quic_0rtt: false,
        }
    }

    #[test]
    fn dashboard_allows_loopback_by_default() {
        let config = DashboardConfig {
            port: 4040,
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            allow_non_loopback: false,
            auth_token: None,
            disabled: false,
        };

        let addr = validate_dashboard_config(&config).expect("loopback bind should be allowed");

        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn dashboard_rejects_non_loopback_without_explicit_opt_in() {
        let config = DashboardConfig {
            port: 4040,
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            allow_non_loopback: false,
            auth_token: None,
            disabled: false,
        };

        let err = validate_dashboard_config(&config).expect_err("non-loopback bind must fail");

        assert!(err.to_string().contains("--dashboard-allow-non-loopback"));
    }

    #[test]
    fn dashboard_rejects_non_loopback_without_auth_token() {
        let config = DashboardConfig {
            port: 4040,
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            allow_non_loopback: true,
            auth_token: None,
            disabled: false,
        };

        let err = validate_dashboard_config(&config).expect_err("exposed bind must require auth");

        assert!(err.to_string().contains("auth token"));
    }

    #[test]
    fn dashboard_allows_non_loopback_with_explicit_opt_in() {
        let config = DashboardConfig {
            port: 4040,
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            allow_non_loopback: true,
            auth_token: Some("secret".to_string()),
            disabled: false,
        };

        let addr =
            validate_dashboard_config(&config).expect("explicit opt-in should allow exposed bind");

        assert!(!addr.ip().is_loopback());
    }

    #[test]
    fn dashboard_rejects_empty_auth_token() {
        let config = DashboardConfig {
            port: 4040,
            bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            allow_non_loopback: false,
            auth_token: Some(String::new()),
            disabled: false,
        };

        let err = validate_dashboard_config(&config).expect_err("empty auth token must fail");

        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn tls_without_ca_requires_explicit_insecure_mode() {
        let args = client_args(true, false, None);

        let err = validate_tls_args(&args).expect_err("TLS without CA must fail");

        assert!(err.to_string().contains("--tls-ca"));
        assert!(err.to_string().contains("--tls-skip-verify"));
    }

    #[test]
    fn tls_allows_explicit_skip_verify_without_ca() {
        let args = client_args(true, true, None);

        validate_tls_args(&args).expect("explicit insecure mode should be allowed");
    }

    #[test]
    fn tls_allows_ca_without_skip_verify() {
        let args = client_args(true, false, Some(PathBuf::from("ca.crt")));

        validate_tls_args(&args).expect("CA-backed TLS should be allowed");
    }
}
