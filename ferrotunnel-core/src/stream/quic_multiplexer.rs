//! QUIC-native stream multiplexer
//!
//! Leverages QUIC's built-in stream multiplexing instead of the custom
//! channel-based [`Multiplexer`](super::Multiplexer). Each data stream maps
//! 1:1 to a QUIC bidirectional stream, eliminating head-of-line blocking.
//!
//! A dedicated control stream (the first bidirectional stream) carries
//! handshake, heartbeat, and other control frames using the existing
//! frame protocol.

use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_protocol::frame::{Protocol, StreamPriority};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A virtual stream backed by a QUIC bidirectional stream.
///
/// Combines `quinn::SendStream` and `quinn::RecvStream` into a single
/// `AsyncRead + AsyncWrite` type, compatible with the rest of the tunnel code.
pub struct QuicVirtualStream {
    stream_id: u32,
    recv: quinn::RecvStream,
    send: quinn::SendStream,
    protocol: Protocol,
}

impl std::fmt::Debug for QuicVirtualStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicVirtualStream")
            .field("stream_id", &self.stream_id)
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

impl QuicVirtualStream {
    pub fn new(
        stream_id: u32,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
        protocol: Protocol,
    ) -> Self {
        Self {
            stream_id,
            recv,
            send,
            protocol,
        }
    }

    pub fn id(&self) -> u32 {
        self.stream_id
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }
}

impl AsyncRead for QuicVirtualStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicVirtualStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.send)
            .poll_write(cx, buf)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send)
            .poll_flush(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.send)
            .poll_shutdown(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))
    }
}

/// QUIC-native multiplexer that uses QUIC streams instead of channel-based multiplexing.
///
/// Each data stream maps to a QUIC bidirectional stream. The control channel
/// (handshake, heartbeat) uses a dedicated stream opened during setup.
#[derive(Clone)]
pub struct QuicMultiplexer {
    connection: quinn::Connection,
    next_stream_id: std::sync::Arc<std::sync::atomic::AtomicU32>,
    rate_limiter: Option<crate::rate_limit::SessionRateLimiter>,
}

impl std::fmt::Debug for QuicMultiplexer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicMultiplexer")
            .field("remote_addr", &self.connection.remote_address())
            .finish_non_exhaustive()
    }
}

impl QuicMultiplexer {
    pub fn new(connection: quinn::Connection, is_client: bool) -> Self {
        let initial_stream_id = if is_client { 1 } else { 2 };
        Self {
            connection,
            next_stream_id: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(
                initial_stream_id,
            )),
            rate_limiter: None,
        }
    }

    /// Apply `limiter` to streams opened through this multiplexer.
    ///
    /// Set by the server for each accepted session. Without it the multiplexer
    /// opens unmetered streams, which is what the client side wants: the budget
    /// is enforced once, by the server.
    #[must_use]
    pub fn with_rate_limiter(mut self, limiter: crate::rate_limit::SessionRateLimiter) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    /// The session rate limiter applied to streams from this multiplexer, if any.
    pub fn rate_limiter(&self) -> Option<&crate::rate_limit::SessionRateLimiter> {
        self.rate_limiter.as_ref()
    }

    /// Allocate a new stream ID atomically.
    ///
    /// Returns an error rather than wrapping past `u32::MAX` so an exhausted ID
    /// space cannot silently alias a live stream.
    fn allocate_stream_id(&self) -> Result<u32> {
        self.next_stream_id
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |id| id.checked_add(2),
            )
            .map_err(|_| TunnelError::Protocol("stream ID space exhausted".into()))
    }

    /// Open a new outbound data stream.
    ///
    /// Opens a QUIC bidirectional stream and writes a 6-byte binary header
    /// so the remote side knows the protocol, priority, and stream ID.
    pub async fn open_stream(&self, protocol: Protocol) -> Result<QuicVirtualStream> {
        self.open_stream_with_priority(protocol, StreamPriority::default())
            .await
    }

    /// Open a new outbound data stream with the given priority.
    pub async fn open_stream_with_priority(
        &self,
        protocol: Protocol,
        priority: StreamPriority,
    ) -> Result<QuicVirtualStream> {
        let stream_id = self.allocate_stream_id()?;

        let (mut send, recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|e| TunnelError::Connection(format!("QUIC open_bi: {e}")))?;

        // Write a fixed-size 6-byte binary header instead of a framed codec
        // to avoid FramedRead buffering that can silently drop stream data.
        let mut header = [0u8; 6];
        header[0] = protocol_to_u8(protocol);
        header[1] = priority.as_u8();
        header[2..6].copy_from_slice(&stream_id.to_be_bytes());
        send.write_all(&header)
            .await
            .map_err(|e| TunnelError::Connection(format!("QUIC write header: {e}")))?;
        Ok(QuicVirtualStream::new(stream_id, send, recv, protocol))
    }

    /// Accept an incoming data stream from the remote peer.
    ///
    /// Reads the 6-byte binary header to determine the protocol, priority, and stream ID.
    pub async fn accept_stream(&self) -> Result<QuicVirtualStream> {
        let (send, mut recv) = self
            .connection
            .accept_bi()
            .await
            .map_err(|e| TunnelError::Connection(format!("QUIC accept_bi: {e}")))?;

        // Read the fixed-size 6-byte binary header directly to avoid
        // FramedRead buffering that can silently drop stream data.
        let mut header = [0u8; 6];
        recv.read_exact(&mut header)
            .await
            .map_err(|e| TunnelError::Connection(format!("QUIC read header: {e}")))?;
        let protocol = u8_to_protocol(header[0]).map_err(|v| {
            TunnelError::Protocol(format!("invalid protocol byte in QUIC stream header: {v}"))
        })?;
        let priority = u8_to_priority(header[1]).map_err(|v| {
            TunnelError::Protocol(format!("invalid priority byte in QUIC stream header: {v}"))
        })?;
        let stream_id = u32::from_be_bytes([header[2], header[3], header[4], header[5]]);

        let _ = priority; // reserved for future QUIC priority support

        Ok(QuicVirtualStream::new(stream_id, send, recv, protocol))
    }

    /// Get the underlying QUIC connection (for control channel setup).
    pub fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    /// Check if the connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.connection.close_reason().is_none()
    }
}

fn protocol_to_u8(p: Protocol) -> u8 {
    match p {
        Protocol::HTTP => 0,
        Protocol::HTTPS => 1,
        Protocol::HTTP2 => 2,
        Protocol::WebSocket => 3,
        Protocol::GRPC => 4,
        Protocol::TCP => 5,
        Protocol::QUIC => 6,
    }
}

fn u8_to_protocol(v: u8) -> std::result::Result<Protocol, u8> {
    match v {
        0 => Ok(Protocol::HTTP),
        1 => Ok(Protocol::HTTPS),
        2 => Ok(Protocol::HTTP2),
        3 => Ok(Protocol::WebSocket),
        4 => Ok(Protocol::GRPC),
        5 => Ok(Protocol::TCP),
        6 => Ok(Protocol::QUIC),
        _ => Err(v),
    }
}

fn u8_to_priority(v: u8) -> std::result::Result<StreamPriority, u8> {
    match v {
        0 => Ok(StreamPriority::Normal),
        1 => Ok(StreamPriority::Low),
        2 => Ok(StreamPriority::High),
        3 => Ok(StreamPriority::Critical),
        _ => Err(v),
    }
}
