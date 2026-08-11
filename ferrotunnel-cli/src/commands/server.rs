//! Server subcommand implementation

use anyhow::{anyhow, Context, Result};
use clap::Args;
use ferrotunnel_core::TunnelServer;
use ferrotunnel_observability::{
    gather_metrics, init_basic_observability, init_minimal_logging, shutdown_tracing,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{error, info};

#[derive(Args, Debug)]
pub struct ServerArgs {
    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:7835", env = "FERROTUNNEL_BIND")]
    bind: SocketAddr,

    /// Read the authentication token from a file. Trailing newlines are ignored.
    #[arg(long, env = "FERROTUNNEL_TOKEN_FILE")]
    token_file: Option<PathBuf>,

    /// Reserved CLI option; set RUST_LOG to configure log filtering
    #[arg(long, default_value = "info", env = "RUST_LOG")]
    log_level: String,

    /// HTTP Ingress bind address
    #[arg(long, default_value = "0.0.0.0:8080", env = "FERROTUNNEL_HTTP_BIND")]
    http_bind: SocketAddr,

    /// Upstream response timeout in seconds for the HTTP/HTTP3 ingress;
    /// raise it above the slowest legitimate upstream response (0 is rejected)
    #[arg(long, default_value = "60", env = "FERROTUNNEL_HTTP_RESPONSE_TIMEOUT")]
    http_response_timeout: u64,

    /// Metrics bind address
    #[arg(long, default_value = "0.0.0.0:9090", env = "FERROTUNNEL_METRICS_BIND")]
    metrics_bind: SocketAddr,

    /// Path to TLS certificate file (PEM format)
    #[arg(long, env = "FERROTUNNEL_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    #[arg(long, env = "FERROTUNNEL_TLS_KEY")]
    tls_key: Option<PathBuf>,

    /// Path to CA certificate for client authentication (PEM format)
    #[arg(long, env = "FERROTUNNEL_TLS_CA")]
    tls_ca: Option<PathBuf>,

    /// Require client certificate authentication
    #[arg(long, env = "FERROTUNNEL_TLS_CLIENT_AUTH")]
    tls_client_auth: bool,

    /// TCP Ingress bind address (optional, for raw TCP tunneling)
    #[arg(long, env = "FERROTUNNEL_TCP_BIND")]
    tcp_bind: Option<SocketAddr>,

    /// HTTP/3 Ingress bind address (UDP)
    #[cfg(feature = "http3")]
    #[arg(long, env = "FERROTUNNEL_HTTP3_BIND")]
    http3_bind: Option<SocketAddr>,

    /// Path to TLS certificate for HTTP/3 (reuses --tls-cert if not specified)
    #[cfg(feature = "http3")]
    #[arg(long, env = "FERROTUNNEL_HTTP3_CERT")]
    http3_cert: Option<PathBuf>,

    /// Path to TLS key for HTTP/3 (reuses --tls-key if not specified)
    #[cfg(feature = "http3")]
    #[arg(long, env = "FERROTUNNEL_HTTP3_KEY")]
    http3_key: Option<PathBuf>,

    /// Enable tracing (metrics is separate via --metrics)
    #[arg(long, env = "FERROTUNNEL_OBSERVABILITY")]
    observability: bool,

    /// Enable metrics endpoint
    #[arg(long, env = "FERROTUNNEL_METRICS")]
    metrics: bool,

    /// QUIC bind address (UDP) for tunnel connections
    #[cfg(feature = "quic")]
    #[arg(long, env = "FERROTUNNEL_QUIC_BIND")]
    quic_bind: Option<SocketAddr>,

    /// Path to TLS certificate for QUIC (reuses --tls-cert if not specified)
    #[cfg(feature = "quic")]
    #[arg(long, env = "FERROTUNNEL_QUIC_CERT")]
    quic_cert: Option<PathBuf>,

    /// Path to TLS key for QUIC (reuses --tls-key if not specified)
    #[cfg(feature = "quic")]
    #[arg(long, env = "FERROTUNNEL_QUIC_KEY")]
    quic_key: Option<PathBuf>,
}

/// Resolve token from env, then token file, then secure prompt.
///
/// The token is deliberately not accepted as a command-line flag: a `--token`
/// argument would be visible to any local user via the process list (`ps`) and
/// could leak into shell history. Supply it via `FERROTUNNEL_TOKEN`,
/// `--token-file`, or the interactive TTY prompt instead.
fn resolve_token(args: &ServerArgs) -> Result<String> {
    if let Ok(t) = std::env::var("FERROTUNNEL_TOKEN") {
        return Ok(t.trim_end_matches(['\r', '\n']).to_owned());
    }
    if let Some(ref path) = args.token_file {
        return read_token_file(path);
    }
    prompt_token()
}

fn read_token_file(path: &Path) -> Result<String> {
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("Could not read token file {}", path.display()))?;
    Ok(token.trim_end_matches(['\r', '\n']).to_owned())
}

/// Prompt for token on TTY without echoing (secure input).
fn prompt_token() -> Result<String> {
    rpassword::prompt_password("Token: ")
        .context("Could not read token from terminal (is stdin a TTY?). Set FERROTUNNEL_TOKEN or pass --token-file")
}

#[allow(clippy::too_many_lines)]
pub async fn run(args: ServerArgs) -> Result<()> {
    validate_args(&args)?;

    let enable_tracing = args.observability;
    let enable_metrics = args.metrics;

    if enable_tracing || enable_metrics {
        init_basic_observability("ferrotunnel-server", enable_tracing, enable_metrics);
    } else {
        init_minimal_logging();
    }

    if enable_metrics {
        // Start metrics endpoint in background (only when metrics is enabled)
        let metrics_addr = args.metrics_bind;
        tokio::spawn(async move {
            use axum::{
                http::StatusCode,
                response::{IntoResponse, Response},
                routing::get,
                Router,
            };
            async fn metrics_response() -> Response {
                match gather_metrics() {
                    Ok(metrics) => (
                        StatusCode::OK,
                        [("content-type", "text/plain; charset=utf-8")],
                        metrics,
                    )
                        .into_response(),
                    Err(error) => {
                        tracing::error!(%error, "failed to gather metrics");
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "failed to gather metrics\n",
                        )
                            .into_response()
                    }
                }
            }

            let app = Router::new()
                .route("/metrics", get(metrics_response))
                .route("/health/ready", get(|| async { "OK" }));
            info!("Metrics server listening on http://{}", metrics_addr);
            match tokio::net::TcpListener::bind(metrics_addr).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, app).await {
                        error!("Metrics server error: {}", e);
                    }
                }
                Err(e) => error!("Failed to bind metrics server to {}: {}", metrics_addr, e),
            }
        });
    }

    info!("Starting FerroTunnel Server v{}", env!("CARGO_PKG_VERSION"));

    let token = resolve_token(&args)?;
    let mut server = TunnelServer::new(args.bind, token.clone());

    if let (Some(cert_path), Some(key_path)) = (&args.tls_cert, &args.tls_key) {
        info!(
            "TLS enabled with cert: {:?}, key: {:?}",
            cert_path, key_path
        );
        server = server.with_tls(cert_path.clone(), key_path.clone());

        if let Some(ca_path) = &args.tls_ca {
            info!("TLS Client Authentication enabled with CA: {:?}", ca_path);
            server = server.with_client_auth(ca_path.clone());
        }
    }
    let sessions = server.sessions();

    // Start Control Plane Service immediately (non-blocking)
    info!("Starting Control Plane on {}", args.bind);
    let server_handle = tokio::spawn(async move { server.run().await });

    info!("Initializing Plugin System");
    let mut registry = ferrotunnel_plugin::PluginRegistry::new();

    // built-in plugins
    // 1. Logger
    registry.register(std::sync::Arc::new(tokio::sync::RwLock::new(
        ferrotunnel_plugin::builtin::LoggerPlugin::new().with_body_logging(),
    )));

    // 2. Token Auth
    registry.register(std::sync::Arc::new(tokio::sync::RwLock::new(
        ferrotunnel_plugin::builtin::TokenAuthPlugin::new(vec![token.clone()]),
    )));

    let registry = std::sync::Arc::new(registry);

    let response_timeout = Duration::from_secs(args.http_response_timeout);

    #[cfg(feature = "http3")]
    let http3_start = if let Some(http3_addr) = args.http3_bind {
        let cert_path = args.http3_cert.clone().or(args.tls_cert.clone());
        let key_path = args.http3_key.clone().or(args.tls_key.clone());

        if let (Some(cert), Some(key)) = (cert_path, key_path) {
            let http3_config = ferrotunnel_http::Http3IngressConfig::with_certs(
                cert.to_string_lossy().to_string(),
                key.to_string_lossy().to_string(),
            )
            .ca_cert_path(
                args.tls_ca
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
            )
            .client_auth(args.tls_client_auth)
            .response_timeout(response_timeout);
            let alt_svc = http3_config.alt_svc_header_value(http3_addr).ok();
            Some((http3_addr, http3_config, alt_svc))
        } else {
            error!("HTTP/3 requires TLS certificates. Provide --http3-cert/--http3-key or --tls-cert/--tls-key");
            None
        }
    } else {
        None
    };

    info!(
        "Starting HTTP Ingress on {} (upstream response timeout: {}s)",
        args.http_bind, args.http_response_timeout
    );
    let ingress_config = ferrotunnel_http::IngressConfig {
        response_timeout,
        ..Default::default()
    };
    let http_ingress = ferrotunnel_http::HttpIngress::with_config(
        args.http_bind,
        sessions.clone(),
        registry.clone(),
        ingress_config,
    );
    #[cfg(feature = "http3")]
    let http_ingress = if let Some((_, _, Some(alt_svc))) = &http3_start {
        http_ingress.with_alt_svc_header(alt_svc.clone())
    } else {
        http_ingress
    };
    let http_handle = tokio::spawn(async move { http_ingress.start().await });

    // Start TCP Ingress (if enabled)
    let tcp_handle = if let Some(tcp_addr) = args.tcp_bind {
        info!("Starting TCP Ingress on {}", tcp_addr);
        let tcp_ingress = ferrotunnel_http::TcpIngress::new(tcp_addr, sessions.clone());
        Some(tokio::spawn(async move { tcp_ingress.start().await }))
    } else {
        None
    };

    #[cfg(feature = "http3")]
    let http3_handle = if let Some((http3_addr, http3_config, _)) = http3_start {
        info!("Starting HTTP/3 Ingress on {} (UDP)", http3_addr);
        let http3_ingress = ferrotunnel_http::Http3Ingress::new(
            http3_addr,
            sessions.clone(),
            registry.clone(),
            http3_config,
        );
        Some(tokio::spawn(async move { http3_ingress.start().await }))
    } else {
        None
    };

    // Start QUIC transport (if enabled)
    #[cfg(feature = "quic")]
    let quic_handle = if let Some(quic_addr) = args.quic_bind {
        let cert_path = args.quic_cert.or(args.tls_cert.clone());
        let key_path = args.quic_key.or(args.tls_key.clone());

        if let (Some(cert), Some(key)) = (cert_path, key_path) {
            info!("Starting QUIC transport on {} (UDP)", quic_addr);
            let quic_config = ferrotunnel_core::transport::quic::QuicTransportConfig {
                cert_path: cert.to_string_lossy().to_string(),
                key_path: key.to_string_lossy().to_string(),
                ca_cert_path: args
                    .tls_ca
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
                ..Default::default()
            };
            let quic_server = TunnelServer::new(args.bind, token.clone())
                .with_transport(ferrotunnel_core::transport::TransportConfig::Quic(
                    quic_config,
                ))
                .with_sessions(sessions.clone());
            Some(tokio::spawn(async move {
                quic_server.run_quic(quic_addr).await
            }))
        } else {
            error!("QUIC requires TLS certificates. Provide --quic-cert/--quic-key or --tls-cert/--tls-key");
            None
        }
    } else {
        None
    };

    // Initialize plugins (after binding ports to avoid "Connection Refused")
    // Incoming requests will wait on the plugin lock if they arrive during init
    registry
        .init_all()
        .await
        .map_err(anyhow::Error::from_boxed)
        .context("Failed to initialize plugins")?;

    // Run services until shutdown signal
    tokio::select! {
        res = async {
            tokio::try_join!(
                async { server_handle.await.map_err(std::io::Error::other)? },
                async { http_handle.await.map_err(std::io::Error::other)? },
                async {
                    if let Some(h) = tcp_handle {
                        h.await.map_err(std::io::Error::other)?
                    } else {
                        Ok(())
                    }
                },
                async {
                    #[cfg(feature = "quic")]
                    if let Some(h) = quic_handle {
                        return h.await.map_err(std::io::Error::other)?;
                    }
                    Ok(())
                },
                async {
                    #[cfg(feature = "http3")]
                    if let Some(h) = http3_handle {
                        return h.await.map_err(std::io::Error::other)?;
                    }
                    Ok(())
                }
            )
        } => {
            res?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, shutting down gracefully...");
            shutdown_tracing();
        }
    }

    Ok(())
}

fn validate_args(args: &ServerArgs) -> Result<()> {
    if args.tls_client_auth && args.tls_ca.is_none() {
        return Err(anyhow!(
            "--tls-client-auth requires --tls-ca to be provided"
        ));
    }

    if args.http_response_timeout == 0 {
        return Err(anyhow!(
            "--http-response-timeout (FERROTUNNEL_HTTP_RESPONSE_TIMEOUT) must be greater than 0"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn server_args() -> ServerArgs {
        ServerArgs {
            bind: SocketAddr::from(([127, 0, 0, 1], 7835)),
            token_file: None,
            log_level: "info".to_string(),
            http_bind: SocketAddr::from(([127, 0, 0, 1], 8080)),
            http_response_timeout: 60,
            metrics_bind: SocketAddr::from(([127, 0, 0, 1], 9090)),
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            tls_client_auth: false,
            tcp_bind: None,
            #[cfg(feature = "http3")]
            http3_bind: None,
            #[cfg(feature = "http3")]
            http3_cert: None,
            #[cfg(feature = "http3")]
            http3_key: None,
            observability: false,
            metrics: false,
            #[cfg(feature = "quic")]
            quic_bind: None,
            #[cfg(feature = "quic")]
            quic_cert: None,
            #[cfg(feature = "quic")]
            quic_key: None,
        }
    }

    #[test]
    fn read_token_file_trims_trailing_line_endings() {
        let path = std::env::temp_dir().join(format!(
            "ferrotunnel-token-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after Unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, "secret\r\n\n").expect("token test file should be writable");

        let token = read_token_file(&path).expect("token file should be readable");
        let _ = std::fs::remove_file(&path);

        assert_eq!(token, "secret");
    }

    #[test]
    fn tls_client_auth_requires_ca() {
        let args = ServerArgs {
            tls_client_auth: true,
            ..server_args()
        };

        let error = validate_args(&args).err().map(|err| err.to_string());

        assert_eq!(
            error.as_deref(),
            Some("--tls-client-auth requires --tls-ca to be provided")
        );
    }

    #[test]
    fn tls_client_auth_accepts_ca() {
        let args = ServerArgs {
            tls_client_auth: true,
            tls_ca: Some(PathBuf::from("ca.pem")),
            ..server_args()
        };

        assert!(validate_args(&args).is_ok());
    }

    #[test]
    fn zero_http_response_timeout_is_rejected() {
        let args = ServerArgs {
            http_response_timeout: 0,
            ..server_args()
        };

        let error = validate_args(&args).err().map(|err| err.to_string());

        assert_eq!(
            error.as_deref(),
            Some("--http-response-timeout (FERROTUNNEL_HTTP_RESPONSE_TIMEOUT) must be greater than 0")
        );
    }

    #[test]
    fn default_http_response_timeout_comes_from_the_parser() {
        use clap::Parser;

        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: ServerArgs,
        }

        let cli = Cli::parse_from(["server"]);
        assert_eq!(cli.args.http_response_timeout, 60);
        assert!(validate_args(&cli.args).is_ok());
    }
}
