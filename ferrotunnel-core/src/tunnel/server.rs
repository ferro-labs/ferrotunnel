use crate::auth::{constant_time_eq, validate_token_format};
use crate::rate_limit::SessionRateLimiter;
use crate::resource_limits::{ServerResourceLimits, SessionPermit};
use crate::stream::{AnyMultiplexer, Multiplexer, PrioritizedFrame};
use crate::transport::batched_sender::run_batched_sender;
use crate::transport::{self, BoxedStream, TransportConfig};
use crate::tunnel::accept_backoff::{is_transient_accept_error, AcceptBackoff};
use crate::tunnel::common::{clamp_u128_to_u64, read_initial_handshake_frame};
use crate::tunnel::session::{Session, SessionStoreBackend, ShardedSessionStore};
use ferrotunnel_common::{LimitsConfig, RateLimitConfig, Result, TunnelError};
use ferrotunnel_protocol::codec::TunnelCodec;
use ferrotunnel_protocol::constants::{MAX_PROTOCOL_VERSION, MIN_PROTOCOL_VERSION};
use ferrotunnel_protocol::frame::{CloseReason, Frame, HandshakeFrame, HandshakeStatus};
use futures::{SinkExt, StreamExt};
use kanal::bounded_async;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
#[cfg(feature = "quic")]
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[cfg(feature = "quic")]
use crate::stream::QuicMultiplexer;
#[cfg(feature = "quic")]
use crate::transport::quic;

const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Interval between periodic stale-session cleanup passes.
const SESSION_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

pub struct TunnelServer {
    addr: SocketAddr,
    auth_token: String,
    sessions: SessionStoreBackend,
    session_timeout: Duration,
    handshake_timeout: Duration,
    resource_limits: ServerResourceLimits,
    transport_config: TransportConfig,
    /// Handle to the periodic session-cleanup task.
    ///
    /// Stored so the task can be aborted on shutdown (see the `Drop` impl) and
    /// guarded so exactly one cleanup loop runs per server instance even if both
    /// `run` and `run_quic` are invoked.
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    limits_config: LimitsConfig,
    rate_limits: RateLimitConfig,
}

impl Drop for TunnelServer {
    fn drop(&mut self) {
        // Abort the cleanup loop so it stops ticking and releases its `Arc`
        // clone of the session store once the server is shut down.
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }
    }
}

impl TunnelServer {
    pub fn new(addr: SocketAddr, auth_token: String) -> Self {
        Self {
            addr,
            auth_token,
            sessions: SessionStoreBackend::default(),
            session_timeout: Duration::from_secs(90),
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            resource_limits: ServerResourceLimits::default(),
            transport_config: TransportConfig::default(),
            cleanup_handle: None,
            limits_config: LimitsConfig::default(),
            rate_limits: RateLimitConfig::default(),
        }
    }

    /// Use a sharded session store for lower contention under many concurrent tunnel_id lookups.
    #[must_use]
    pub fn with_sharded_sessions(mut self, n_shards: usize) -> Self {
        self.sessions = SessionStoreBackend::Sharded(ShardedSessionStore::with_shards(n_shards));
        self
    }

    #[must_use]
    pub fn with_resource_limits(mut self, limits: ServerResourceLimits) -> Self {
        self.resource_limits = limits;
        self
    }

    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set the resource limits used to build the frame codec (frame size and
    /// control-frame validation bounds).
    #[must_use]
    pub fn with_limits(mut self, limits: LimitsConfig) -> Self {
        self.limits_config = limits;
        self
    }

    /// Set the per-session rate limits applied to every accepted session.
    ///
    /// Over-budget data frames are throttled (delayed and delivered) rather
    /// than dropped. Note that a very low byte rate combined with a large
    /// `max_frame_bytes` can park a session for a long time: a frame larger
    /// than the burst capacity waits for quota chunk by chunk. Each chunk is
    /// bounded to roughly thirty seconds of quota, and the session heartbeat
    /// is refreshed between chunks, so a long throttle is never evicted as
    /// stale regardless of the configured rate.
    #[must_use]
    pub fn with_rate_limits(mut self, limits: RateLimitConfig) -> Self {
        self.rate_limits = limits;
        self
    }

    #[must_use]
    pub fn with_transport(mut self, config: TransportConfig) -> Self {
        self.transport_config = config;
        self
    }

    /// Reuse an existing session backend so multiple listeners share tunnel state.
    #[must_use]
    pub fn with_sessions(mut self, sessions: SessionStoreBackend) -> Self {
        self.sessions = sessions;
        self
    }

    /// Configure TLS for the server using certificate and key files.
    #[must_use]
    pub fn with_tls(
        mut self,
        cert_path: impl Into<std::path::PathBuf>,
        key_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        let (cert, key) = (
            cert_path.into().to_string_lossy().to_string(),
            key_path.into().to_string_lossy().to_string(),
        );

        if let TransportConfig::Tls(ref mut tls) = self.transport_config {
            tls.cert_path = cert;
            tls.key_path = key;
        } else {
            self.transport_config = TransportConfig::Tls(transport::tls::TlsTransportConfig {
                cert_path: cert,
                key_path: key,
                ..Default::default()
            });
        }
        self
    }

    /// Enable client certificate authentication (Mutual TLS) using the provided CA.
    #[must_use]
    pub fn with_client_auth(mut self, ca_path: impl Into<std::path::PathBuf>) -> Self {
        let ca = Some(ca_path.into().to_string_lossy().to_string());
        if let TransportConfig::Tls(ref mut tls) = self.transport_config {
            tls.ca_cert_path = ca;
            tls.client_auth = true;
        } else {
            self.transport_config = TransportConfig::Tls(transport::tls::TlsTransportConfig {
                ca_cert_path: ca,
                client_auth: true,
                ..Default::default()
            });
        }
        self
    }

    pub fn sessions(&self) -> SessionStoreBackend {
        self.sessions.clone()
    }

    /// Start the periodic stale-session cleanup task exactly once.
    ///
    /// The `JoinHandle` is stored on the server so the loop can be aborted when
    /// the server is dropped. If a task is already running this is a no-op,
    /// guaranteeing a single cleanup loop per server instance even when both
    /// `run` and `run_quic` are called.
    fn start_cleanup_task(&mut self) {
        if self.cleanup_handle.is_some() {
            return;
        }

        let cleanup_sessions = self.sessions.clone();
        let timeout = self.session_timeout;
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(SESSION_CLEANUP_INTERVAL);
            loop {
                interval.tick().await;
                let count = cleanup_sessions.cleanup_stale_sessions(timeout);
                if count > 0 {
                    info!("Cleaned up {} stale sessions", count);
                }
            }
        });
        self.cleanup_handle = Some(handle);
    }

    pub async fn run(mut self) -> Result<()> {
        crate::limits::validate_limits(&self.limits_config)?;
        crate::limits::validate_rate_limits(&self.rate_limits)?;
        let listener = TcpListener::bind(self.addr).await?;
        info!("Server listening on {}", self.addr);

        self.start_cleanup_task();

        let sessions = self.sessions.clone();
        let handshake_timeout = self.handshake_timeout;

        // Back off on persistent accept errors (e.g. a failing TLS acceptor or
        // `EMFILE`) so the loop does not busy-spin at 100% CPU and flood logs.
        let mut accept_backoff = AcceptBackoff::default();
        loop {
            let (stream, addr) = match transport::accept(&self.transport_config, &listener).await {
                Ok(connection) => {
                    accept_backoff.reset();
                    connection
                }
                // `transport::accept` performs the per-connection TLS handshake,
                // so an error here is almost always a single bad/half-open
                // connection (e.g. a plain-TCP health probe against a TLS
                // listener), not a broken listener. Never terminate the accept
                // loop; log (rate-limited) and back off so a persistent error
                // (failing acceptor, EMFILE) cannot busy-spin a core.
                Err(e) => {
                    let (delay, should_log) = accept_backoff.record_failure();
                    if should_log {
                        warn!(
                            bind_addr = %self.addr,
                            error = %e,
                            transient = is_transient_accept_error(&e),
                            backoff_ms = delay.as_millis(),
                            "Accept error; backing off"
                        );
                    }
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            let session_permit = match self.resource_limits.try_acquire_session() {
                Ok(permit) => permit,
                Err(e) => {
                    warn!("Rejecting connection from {}: {}", addr, e);
                    continue;
                }
            };

            let sessions = sessions.clone();
            let token = self.auth_token.clone();
            let limits = self.limits_config.clone();
            let rate_limits = self.rate_limits.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(
                    stream,
                    addr,
                    sessions,
                    token,
                    session_permit,
                    handshake_timeout,
                    limits,
                    rate_limits,
                )
                .await
                {
                    warn!("Connection error for {}: {}", addr, e);
                }
            });
        }
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn handle_connection(
        stream: BoxedStream,
        addr: SocketAddr,
        sessions: SessionStoreBackend,
        expected_token: String,
        _session_permit: SessionPermit,
        handshake_timeout: Duration,
        limits_config: LimitsConfig,
        rate_limits: RateLimitConfig,
    ) -> Result<()> {
        let mut framed = Framed::new(stream, TunnelCodec::from_limits(&limits_config));

        // 1. Handshake
        let frame = read_initial_handshake_frame(&mut framed, handshake_timeout, "server").await?;
        match frame {
            Frame::Handshake(handshake) => {
                let auth = match authenticate_handshake(*handshake, &expected_token, addr) {
                    Ok(auth) => auth,
                    Err(status) => {
                        framed
                            .send(Frame::HandshakeAck {
                                status,
                                session_id: Uuid::nil(),
                                version: 0,
                                server_capabilities: vec![],
                            })
                            .await?;
                        return Ok(());
                    }
                };

                let AuthenticatedHandshake {
                    session_id,
                    tunnel_id,
                    negotiated_version,
                    token,
                    capabilities,
                } = auth;

                // Setup multiplexer with kanal channels
                let parts = framed.into_parts();
                let (read_half, write_half) = tokio::io::split(parts.io);

                // Reconstruct FramedRead for reading, preserving any buffered data.
                // Dropping read_buf causes decoder desync ("Frame too large: 2021161080").
                let mut stream = tokio_util::codec::FramedRead::new(read_half, parts.codec);
                if !parts.read_buf.is_empty() {
                    stream.read_buffer_mut().extend_from_slice(&parts.read_buf);
                }

                let (frame_tx, frame_rx) = bounded_async::<PrioritizedFrame>(1024);

                // Spawn batched sender task for vectored I/O performance
                tokio::spawn(run_batched_sender(frame_rx, write_half, parts.codec));

                let (multiplexer, new_stream_rx) = Multiplexer::new(frame_tx, false);

                // Log unexpected streams from client (for now)
                tokio::spawn(async move {
                    while let Ok(_stream) = new_stream_rx.recv().await {
                        warn!("Client tried to open stream (not supported in MVP)");
                    }
                });

                let session = Session::new(
                    session_id,
                    tunnel_id.clone(),
                    addr,
                    token,
                    capabilities,
                    Some(AnyMultiplexer::Tcp(multiplexer.clone())),
                )
                .with_rate_limiter(SessionRateLimiter::new(&rate_limits.into()));

                if let Err(e) = sessions.add(session) {
                    warn!("Failed to register session: {}", e);
                    multiplexer
                        .send_frame(Frame::HandshakeAck {
                            status: HandshakeStatus::TunnelIdTaken,
                            session_id,
                            version: 0,
                            server_capabilities: vec![],
                        })
                        .await?;
                    return Err(TunnelError::Protocol(format!(
                        "Tunnel ID '{tunnel_id}' already in use"
                    )));
                }

                info!("Session established: {}", session_id);
                multiplexer
                    .send_frame(Frame::HandshakeAck {
                        status: HandshakeStatus::Success,
                        session_id,
                        version: negotiated_version,
                        server_capabilities: vec!["basic".to_string()],
                    })
                    .await?;

                // Enter message loop
                Self::process_messages(stream, session_id, sessions, multiplexer).await?;
            }
            _ => {
                return Err(TunnelError::Protocol("Expected handshake".into()));
            }
        }

        Ok(())
    }

    async fn process_messages(
        mut stream: tokio_util::codec::FramedRead<tokio::io::ReadHalf<BoxedStream>, TunnelCodec>,
        session_id: Uuid,
        sessions: SessionStoreBackend,
        multiplexer: Multiplexer,
    ) -> Result<()> {
        let mut throttling = false;
        loop {
            #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
            let decode_start = Instant::now();
            let result = stream.next().await;
            let Some(frame_result) = result else { break };
            let frame = frame_result?;

            #[cfg(feature = "metrics")]
            if let Some(m) = ferrotunnel_observability::tunnel_metrics() {
                let bytes = match &frame {
                    Frame::Data { data, .. } => data.len(),
                    _ => 0,
                };
                m.record_decode(1, bytes, decode_start.elapsed());
            }

            // Update heartbeat for any activity and snapshot the rate limiter.
            let rate_limiter = if let Some(mut session) = sessions.get_mut(&session_id) {
                session.update_heartbeat();
                session.rate_limiter.clone()
            } else {
                // Session removed (shutdown/timeout)
                return Err(TunnelError::Protocol("Session not found".into()));
            };

            // Enforce per-session rate limits on the hot path (issue #119).
            // Deny stream opens when over limit; throttle over-budget data frames
            // instead of dropping them so the tunneled byte stream stays intact.
            if let Some(limiter) = &rate_limiter {
                match &frame {
                    Frame::OpenStream(open) if limiter.check_stream_open().is_err() => {
                        let stream_id = open.stream_id;
                        warn!(
                            session_id = %session_id,
                            stream_id,
                            "Stream-open rate limit exceeded; denying stream"
                        );
                        // Signal denial to the client so it does not wait indefinitely.
                        let _ = multiplexer
                            .send_frame(Frame::CloseStream {
                                stream_id,
                                reason: CloseReason::Error("rate limited".into()),
                            })
                            .await;
                        continue;
                    }
                    Frame::Data { data, .. } if limiter.check_data(data.len()).is_err() => {
                        if !throttling {
                            throttling = true;
                            warn!(
                                session_id = %session_id,
                                bytes = data.len(),
                                "Data rate limit exceeded; throttling session"
                            );
                        }
                        debug!(
                            session_id = %session_id,
                            bytes = data.len(),
                            "Throttling over-budget data frame"
                        );
                        // Deliberate head-of-line blocking: the byte budget is
                        // session-wide, so every stream on this session stalls while
                        // one is over budget; backpressure reaches the client through
                        // TCP flow control instead of dropping frames mid-stream.
                        limiter
                            .throttle_data(data.len(), || {
                                // Touch the heartbeat so a long throttle is not evicted as stale.
                                if let Some(mut session) = sessions.get_mut(&session_id) {
                                    session.update_heartbeat();
                                }
                            })
                            .await;
                    }
                    Frame::Data { .. } => throttling = false,
                    _ => {}
                }
            }

            match frame {
                Frame::Heartbeat { .. } => {
                    multiplexer
                        .send_frame(Frame::HeartbeatAck {
                            timestamp: clamp_u128_to_u64(
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis(),
                            ),
                        })
                        .await?;
                }
                _ => {
                    multiplexer.process_frame(frame).await?;
                }
            }
        }

        info!("Client disconnected: {}", session_id);
        sessions.remove(&session_id);
        Ok(())
    }
}

#[cfg(feature = "quic")]
impl TunnelServer {
    /// Run the QUIC transport server on the given UDP address.
    ///
    /// This is separate from `run()` which uses TCP. Both can run simultaneously
    /// on different ports.
    pub async fn run_quic(mut self, quic_addr: SocketAddr) -> Result<()> {
        crate::limits::validate_limits(&self.limits_config)?;
        crate::limits::validate_rate_limits(&self.rate_limits)?;
        let quic_config = match &self.transport_config {
            TransportConfig::Quic(c) => c.clone(),
            _ => {
                return Err(TunnelError::Config(
                    "QUIC transport config required for run_quic()".into(),
                ));
            }
        };

        let endpoint = quic::create_server_endpoint(&quic_config, quic_addr)?;
        info!("QUIC server listening on {}", quic_addr);

        self.start_cleanup_task();

        let sessions = self.sessions.clone();
        let handshake_timeout = self.handshake_timeout;

        // Back off on persistent accept errors so the loop does not busy-spin
        // at 100% CPU and flood logs (e.g. `EMFILE` while fds stay exhausted).
        let mut accept_backoff = AcceptBackoff::default();
        loop {
            let (connection, addr) = match quic::accept(&endpoint).await {
                Ok(accepted) => {
                    accept_backoff.reset();
                    accepted
                }
                // QUIC acceptance includes the per-connection handshake, so an
                // error is almost always one bad connection, not a dead
                // endpoint. Never terminate the loop; log (rate-limited) and
                // back off so a persistent failure cannot busy-spin a core.
                Err(e) => {
                    let (delay, should_log) = accept_backoff.record_failure();
                    if should_log {
                        warn!(
                            bind_addr = %quic_addr,
                            error = %e,
                            transient = is_transient_accept_error(&e),
                            backoff_ms = delay.as_millis(),
                            "QUIC accept error; backing off"
                        );
                    }
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            let session_permit = match self.resource_limits.try_acquire_session() {
                Ok(permit) => permit,
                Err(e) => {
                    warn!("Rejecting QUIC connection from {}: {}", addr, e);
                    continue;
                }
            };

            let sessions = sessions.clone();
            let token = self.auth_token.clone();
            let limits = self.limits_config.clone();
            let rate_limits = self.rate_limits.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_quic_connection(
                    connection,
                    addr,
                    sessions,
                    token,
                    session_permit,
                    handshake_timeout,
                    limits,
                    rate_limits,
                )
                .await
                {
                    warn!("QUIC connection error for {}: {}", addr, e);
                }
            });
        }
    }

    /// Handle a single QUIC connection.
    ///
    /// Uses the first bidirectional stream as a control channel for handshake and heartbeats.
    /// Subsequent bidirectional streams carry data (each QUIC stream = one tunnel stream).
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn handle_quic_connection(
        connection: quinn::Connection,
        addr: SocketAddr,
        sessions: SessionStoreBackend,
        expected_token: String,
        _session_permit: SessionPermit,
        handshake_timeout: Duration,
        limits_config: LimitsConfig,
        rate_limits: RateLimitConfig,
    ) -> Result<()> {
        // Accept the control stream (first bidi stream opened by client)
        let (ctrl_send, ctrl_recv) = match timeout(handshake_timeout, connection.accept_bi()).await
        {
            Ok(Ok(streams)) => streams,
            Ok(Err(e)) => {
                return Err(TunnelError::Connection(format!(
                    "QUIC control stream accept: {e}"
                )));
            }
            Err(_) => {
                connection.close(quinn::VarInt::from_u32(0), b"handshake timeout");
                return Err(TunnelError::Timeout(format!(
                    "QUIC control stream not opened within {handshake_timeout:?}"
                )));
            }
        };

        let mut ctrl_framed_recv =
            tokio_util::codec::FramedRead::new(ctrl_recv, TunnelCodec::from_limits(&limits_config));
        let mut ctrl_framed_send = tokio_util::codec::FramedWrite::new(
            ctrl_send,
            TunnelCodec::from_limits(&limits_config),
        );

        // 1. Handshake on control stream
        let frame = match read_initial_handshake_frame(
            &mut ctrl_framed_recv,
            handshake_timeout,
            "QUIC server",
        )
        .await
        {
            Ok(frame) => frame,
            Err(err @ TunnelError::Timeout(_)) => {
                connection.close(quinn::VarInt::from_u32(0), b"handshake timeout");
                return Err(err);
            }
            Err(err) => return Err(err),
        };

        let (session_id, tunnel_id, multiplexer) = match frame {
            Frame::Handshake(handshake) => {
                let auth = match authenticate_handshake(*handshake, &expected_token, addr) {
                    Ok(auth) => auth,
                    Err(status) => {
                        ctrl_framed_send
                            .send(Frame::HandshakeAck {
                                status,
                                session_id: Uuid::nil(),
                                version: 0,
                                server_capabilities: vec![],
                            })
                            .await?;
                        return Ok(());
                    }
                };

                let AuthenticatedHandshake {
                    session_id,
                    tunnel_id,
                    negotiated_version,
                    token,
                    capabilities,
                } = auth;

                // Create QUIC multiplexer for data streams
                let quic_mux = QuicMultiplexer::new(connection.clone(), false);

                let session = Session::new(
                    session_id,
                    tunnel_id.clone(),
                    addr,
                    token,
                    capabilities,
                    Some(AnyMultiplexer::Quic(quic_mux.clone())),
                )
                .with_rate_limiter(SessionRateLimiter::new(&rate_limits.into()));

                if let Err(e) = sessions.add(session) {
                    warn!("Failed to register QUIC session: {}", e);
                    ctrl_framed_send
                        .send(Frame::HandshakeAck {
                            status: HandshakeStatus::TunnelIdTaken,
                            session_id,
                            version: 0,
                            server_capabilities: vec![],
                        })
                        .await?;
                    return Err(TunnelError::Protocol(format!(
                        "Tunnel ID '{tunnel_id}' already in use"
                    )));
                }

                info!("QUIC session established: {}", session_id);
                ctrl_framed_send
                    .send(Frame::HandshakeAck {
                        status: HandshakeStatus::Success,
                        session_id,
                        version: negotiated_version,
                        server_capabilities: vec!["basic".to_string(), "quic".to_string()],
                    })
                    .await?;

                (session_id, tunnel_id, quic_mux)
            }
            _ => {
                return Err(TunnelError::Protocol(
                    "Expected handshake on QUIC control stream".into(),
                ));
            }
        };

        // 2. Run control loop (heartbeats) and data stream acceptance in parallel
        let ctrl_result = tokio::select! {
            result = Self::quic_control_loop(
                &mut ctrl_framed_recv,
                &mut ctrl_framed_send,
                session_id,
                &sessions,
            ) => result,
            result = Self::quic_data_stream_loop(&multiplexer) => result,
            _ = connection.closed() => {
                info!("QUIC connection closed for session {}", session_id);
                Ok(())
            }
        };

        // Cleanup
        sessions.remove(&session_id);
        info!(
            "QUIC client disconnected: {} (tunnel: {})",
            session_id, tunnel_id
        );
        ctrl_result
    }

    /// Process control frames (heartbeat/ack) on the dedicated control stream.
    async fn quic_control_loop(
        ctrl_recv: &mut tokio_util::codec::FramedRead<quinn::RecvStream, TunnelCodec>,
        ctrl_send: &mut tokio_util::codec::FramedWrite<quinn::SendStream, TunnelCodec>,
        session_id: Uuid,
        sessions: &SessionStoreBackend,
    ) -> Result<()> {
        loop {
            let frame = ctrl_recv
                .next()
                .await
                .ok_or_else(|| TunnelError::Connection("QUIC control stream closed".into()))?
                .map_err(TunnelError::Io)?;

            // Update heartbeat for any activity
            if let Some(mut session) = sessions.get_mut(&session_id) {
                session.update_heartbeat();
            } else {
                return Err(TunnelError::Protocol("Session not found".into()));
            }

            match frame {
                Frame::Heartbeat { .. } => {
                    let ts = clamp_u128_to_u64(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis(),
                    );
                    ctrl_send
                        .send(Frame::HeartbeatAck { timestamp: ts })
                        .await?;
                }
                _ => {
                    warn!("Unexpected frame on QUIC control stream: {:?}", frame);
                }
            }
        }
    }

    /// Accept data streams from QUIC and log them.
    /// In the full implementation, these streams are used by the HTTP ingress
    /// to forward requests to the tunnel client.
    async fn quic_data_stream_loop(multiplexer: &QuicMultiplexer) -> Result<()> {
        loop {
            match multiplexer.accept_stream().await {
                Ok(_stream) => {
                    warn!("Server received data stream from QUIC client (not expected in current flow)");
                }
                Err(e) => {
                    // Connection closed
                    return Err(e);
                }
            }
        }
    }
}

/// Result of a successful handshake authentication.
struct AuthenticatedHandshake {
    session_id: Uuid,
    tunnel_id: String,
    negotiated_version: u8,
    token: String,
    capabilities: Vec<String>,
}

/// Validate and authenticate a handshake frame.
/// Returns the authenticated handshake info, or a HandshakeStatus error.
fn authenticate_handshake(
    handshake: HandshakeFrame,
    expected_token: &str,
    addr: SocketAddr,
) -> std::result::Result<AuthenticatedHandshake, HandshakeStatus> {
    let HandshakeFrame {
        min_version,
        max_version,
        token,
        tunnel_id,
        capabilities,
    } = handshake;

    if let Err(e) = validate_token_format(&token, 256) {
        warn!("Invalid token format from {}: {}", addr, e);
        return Err(HandshakeStatus::InvalidToken);
    }

    if !constant_time_eq(token.as_bytes(), expected_token.as_bytes()) {
        warn!("Invalid token from {}", addr);
        return Err(HandshakeStatus::InvalidToken);
    }

    let negotiated_version = match negotiate_version(min_version, max_version) {
        Ok(v) => v,
        Err(e) => {
            warn!("Version negotiation failed for {}: {}", addr, e);
            return Err(HandshakeStatus::VersionMismatch);
        }
    };

    info!(
        "Client {} supports v{}-{}, negotiated v{}",
        addr, min_version, max_version, negotiated_version
    );

    let session_id = Uuid::new_v4();
    let tunnel_id = tunnel_id.unwrap_or_else(|| session_id.to_string());

    Ok(AuthenticatedHandshake {
        session_id,
        tunnel_id,
        negotiated_version,
        token,
        capabilities,
    })
}

/// Negotiate protocol version between client and server
fn negotiate_version(client_min: u8, client_max: u8) -> Result<u8> {
    // Find highest common version
    let server_min = MIN_PROTOCOL_VERSION;
    let server_max = MAX_PROTOCOL_VERSION;

    if client_max < server_min || client_min > server_max {
        // No overlap
        return Err(TunnelError::Protocol(format!(
            "No compatible protocol version. Server: {server_min}-{server_max}, Client: {client_min}-{client_max}"
        )));
    }

    // Use highest common version
    let negotiated = std::cmp::min(client_max, server_max);
    Ok(negotiated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_negotiation_success() {
        // Client supports 1-2, Server supports 1-1 → v1
        assert_eq!(negotiate_version(1, 2).unwrap(), 1);

        // Client supports 1-1, Server supports 1-2 → v1
        assert_eq!(negotiate_version(1, 1).unwrap(), 1);
    }

    #[test]
    fn test_version_negotiation_failure() {
        // Client requires 3+, Server only has 1
        assert!(negotiate_version(3, 5).is_err());
    }

    #[tokio::test]
    async fn handshake_timeout_releases_session_permit_for_silent_peer() {
        let (server_side, _client_side) = tokio::io::duplex(1024);
        let limits = ServerResourceLimits::new(1, 10, 100);
        let permit = limits.try_acquire_session().unwrap();
        assert_eq!(limits.available_sessions(), 0);

        let stream: BoxedStream = Box::pin(server_side);
        let err = TunnelServer::handle_connection(
            stream,
            "127.0.0.1:0".parse().unwrap(),
            SessionStoreBackend::default(),
            "token".to_string(),
            permit,
            Duration::from_millis(1),
            LimitsConfig::default(),
            RateLimitConfig::default(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            TunnelError::Timeout(msg) if msg.contains("server handshake timed out")
        ));
        assert_eq!(limits.available_sessions(), 1);
    }

    #[tokio::test]
    async fn run_rejects_unrepresentable_rate_limits_before_binding() {
        // Port 0 would bind successfully, so reaching an error at all proves
        // validation runs ahead of the listener rather than after it.
        let server = TunnelServer::new("127.0.0.1:0".parse().unwrap(), "token".to_string())
            .with_rate_limits(RateLimitConfig {
                bytes_per_sec: u64::from(u32::MAX) + 1,
                ..Default::default()
            });

        let err = server
            .run()
            .await
            .expect_err("a byte rate the limiter cannot represent must be rejected");

        assert!(
            matches!(err, TunnelError::Config(ref msg) if msg.contains("bytes_per_sec")),
            "error should identify the field: {err}"
        );
    }

    #[tokio::test]
    async fn cleanup_task_spawns_only_once() {
        let mut server = TunnelServer::new("127.0.0.1:0".parse().unwrap(), "token".to_string());
        assert!(server.cleanup_handle.is_none());

        server.start_cleanup_task();
        let first_id = server
            .cleanup_handle
            .as_ref()
            .expect("cleanup task should be spawned")
            .id();

        // A second call (e.g. if both run() and run_quic() execute) must not
        // spawn a competing cleanup loop.
        server.start_cleanup_task();
        let second_id = server
            .cleanup_handle
            .as_ref()
            .expect("cleanup task should still be present")
            .id();

        assert_eq!(first_id, second_id, "a second cleanup loop was spawned");
    }

    #[tokio::test]
    async fn cleanup_task_aborted_when_server_dropped() {
        let mut server = TunnelServer::new("127.0.0.1:0".parse().unwrap(), "token".to_string());
        server.start_cleanup_task();

        let abort = server
            .cleanup_handle
            .as_ref()
            .expect("cleanup task should be spawned")
            .abort_handle();
        assert!(!abort.is_finished());

        // Dropping the server simulates shutdown; the cleanup task must stop.
        drop(server);

        // The abort is processed asynchronously; poll until the task stops.
        for _ in 0..100 {
            if abort.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            abort.is_finished(),
            "cleanup task should be aborted after server drop"
        );
    }

    #[tokio::test]
    async fn process_messages_enforces_stream_open_rate_limit() {
        use crate::rate_limit::{RateLimiterConfig, SessionRateLimiter};
        use ferrotunnel_protocol::frame::{OpenStreamFrame, Protocol, StreamPriority};
        use std::num::NonZeroU32;

        // Duplex channel: the "client" half writes frames the server reads.
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);

        // Multiplexer wired to a drained outbound channel so denial frames never block.
        let (frame_tx, frame_rx) = bounded_async::<PrioritizedFrame>(1024);
        tokio::spawn(async move { while frame_rx.recv().await.is_ok() {} });
        let (multiplexer, new_stream_rx) = Multiplexer::new(frame_tx, false);

        // Session with a tight stream-open limiter: capacity = 1 (1/s, burst 1).
        let session_id = Uuid::new_v4();
        let tight = RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(1).unwrap(),
            bytes_per_sec: NonZeroU32::new(1000).unwrap(),
            burst_factor: NonZeroU32::new(1).unwrap(),
        };
        let sessions = SessionStoreBackend::default();
        let session = Session::new(
            session_id,
            "tight-tunnel".into(),
            "127.0.0.1:0".parse().unwrap(),
            "tok".into(),
            vec![],
            Some(AnyMultiplexer::Tcp(multiplexer.clone())),
        )
        .with_rate_limiter(SessionRateLimiter::new(&tight));
        sessions.add(session).unwrap();

        // Client encodes three OpenStream frames, then closes (EOF ends the loop).
        let mut client = Framed::new(client_side, TunnelCodec::new());
        for stream_id in [1u32, 3, 5] {
            client
                .send(Frame::OpenStream(Box::new(OpenStreamFrame {
                    stream_id,
                    protocol: Protocol::TCP,
                    headers: vec![],
                    body_hint: None,
                    priority: StreamPriority::default(),
                })))
                .await
                .unwrap();
        }
        client.flush().await.unwrap();
        drop(client);

        // Drive the production hot path.
        let server_stream: BoxedStream = Box::pin(server_side);
        let (read_half, _write_half) = tokio::io::split(server_stream);
        let framed_read = tokio_util::codec::FramedRead::new(read_half, TunnelCodec::new());
        TunnelServer::process_messages(framed_read, session_id, sessions, multiplexer)
            .await
            .unwrap();

        // Only the first stream-open passes enforcement; the rest are denied.
        let mut accepted = 0;
        while new_stream_rx.try_recv().ok().flatten().is_some() {
            accepted += 1;
        }
        assert_eq!(
            accepted, 1,
            "rate limiter must deny stream opens over limit"
        );
    }

    #[tokio::test]
    async fn process_messages_enforces_data_byte_rate_limit() {
        use crate::rate_limit::{RateLimiterConfig, SessionRateLimiter};
        use bytes::Bytes;
        use ferrotunnel_protocol::frame::{OpenStreamFrame, Protocol, StreamPriority};
        use std::num::NonZeroU32;
        use tokio::io::AsyncReadExt;
        use tokio::time::timeout;

        let (client_side, server_side) = tokio::io::duplex(64 * 1024);

        let (frame_tx, frame_rx) = bounded_async::<PrioritizedFrame>(1024);
        tokio::spawn(async move { while frame_rx.recv().await.is_ok() {} });
        let (multiplexer, new_stream_rx) = Multiplexer::new(frame_tx, false);

        // Generous stream-open budget but a byte budget of exactly one frame
        // (capacity = 8 bytes) so the first data frame consumes the whole quota
        // and the second is over budget.
        let session_id = Uuid::new_v4();
        let tight = RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(100).unwrap(),
            bytes_per_sec: NonZeroU32::new(8).unwrap(),
            burst_factor: NonZeroU32::new(1).unwrap(),
        };
        let sessions = SessionStoreBackend::default();
        let session = Session::new(
            session_id,
            "tight-bytes".into(),
            "127.0.0.1:0".parse().unwrap(),
            "tok".into(),
            vec![],
            Some(AnyMultiplexer::Tcp(multiplexer.clone())),
        )
        .with_rate_limiter(SessionRateLimiter::new(&tight));
        sessions.add(session).unwrap();

        let mut client = Framed::new(client_side, TunnelCodec::new());
        // Open stream 1, send 8 bytes (the whole 8-byte budget), then 8 more
        // bytes (over budget -> must be throttled by the Frame::Data limiter arm).
        client
            .send(Frame::OpenStream(Box::new(OpenStreamFrame {
                stream_id: 1,
                protocol: Protocol::TCP,
                headers: vec![],
                body_hint: None,
                priority: StreamPriority::default(),
            })))
            .await
            .unwrap();
        for payload in [&b"AAAAAAAA"[..], &b"BBBBBBBB"[..]] {
            client
                .send(Frame::Data {
                    stream_id: 1,
                    data: Bytes::copy_from_slice(payload),
                    end_of_stream: false,
                })
                .await
                .unwrap();
        }
        client.flush().await.unwrap();
        drop(client);

        let server_stream: BoxedStream = Box::pin(server_side);
        let (read_half, _write_half) = tokio::io::split(server_stream);
        let framed_read = tokio_util::codec::FramedRead::new(read_half, TunnelCodec::new());
        // Keep a multiplexer clone alive so the opened stream's channel stays open
        // after `process_messages` returns; otherwise a lost (rate-limited) frame
        // would be indistinguishable from channel-close EOF.
        let _keepalive = multiplexer.clone();
        let started = Instant::now();
        TunnelServer::process_messages(framed_read, session_id, sessions, multiplexer)
            .await
            .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(500),
            "over-budget data frame must be throttled (delayed), got {:?}",
            started.elapsed()
        );

        // The opened stream receives both data frames intact and in order; the
        // second is delayed by the throttle instead of dropped.
        let mut stream = new_stream_rx.recv().await.unwrap();
        let mut buf = vec![0u8; 64];
        let first = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..first], b"AAAAAAAA");
        let second = timeout(Duration::from_millis(300), stream.read(&mut buf))
            .await
            .expect("over-budget data frame must be delivered after throttling, not dropped")
            .unwrap();
        assert_eq!(&buf[..second], b"BBBBBBBB");
    }
}
