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
use ferrotunnel_protocol::codec::TunnelCodec;
use ferrotunnel_protocol::frame::{Frame, OpenStreamFrame, Protocol, StreamPriority};
use futures::{SinkExt, StreamExt};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::warn;

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
        }
    }

    /// Allocate a new stream ID atomically.
    fn allocate_stream_id(&self) -> u32 {
        self.next_stream_id
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed)
    }

    /// Open a new outbound data stream.
    ///
    /// Opens a QUIC bidirectional stream and sends an `OpenStreamFrame` as the
    /// first message so the remote side knows the protocol and stream ID.
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
        let stream_id = self.allocate_stream_id();

        let (send, recv) = self.connection.open_bi().await.map_err(|e| {
            TunnelError::Connection(format!("QUIC open_bi: {e}"))
        })?;

        // Send OpenStreamFrame as the first message on this stream
        let mut framed_send = FramedWrite::new(send, TunnelCodec::new());
        framed_send
            .send(Frame::OpenStream(Box::new(OpenStreamFrame {
                stream_id,
                protocol,
                headers: vec![],
                body_hint: None,
                priority,
            })))
            .await?;

        let send = framed_send.into_inner();
        Ok(QuicVirtualStream::new(stream_id, send, recv, protocol))
    }

    /// Accept an incoming data stream from the remote peer.
    ///
    /// Reads the initial `OpenStreamFrame` to determine the protocol and stream ID.
    pub async fn accept_stream(&self) -> Result<QuicVirtualStream> {
        let (send, recv) = self.connection.accept_bi().await.map_err(|e| {
            TunnelError::Connection(format!("QUIC accept_bi: {e}"))
        })?;

        // Read the OpenStreamFrame from the first message
        let mut framed_recv = FramedRead::new(recv, TunnelCodec::new());
        let frame = framed_recv
            .next()
            .await
            .ok_or_else(|| TunnelError::Protocol("QUIC stream closed before OpenStreamFrame".into()))?
            .map_err(TunnelError::Io)?;

        match frame {
            Frame::OpenStream(open) => {
                let recv = framed_recv.into_inner();
                Ok(QuicVirtualStream::new(
                    open.stream_id,
                    send,
                    recv,
                    open.protocol,
                ))
            }
            other => {
                warn!("Expected OpenStreamFrame on QUIC stream, got {:?}", other);
                Err(TunnelError::Protocol(
                    "Expected OpenStreamFrame on QUIC data stream".into(),
                ))
            }
        }
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
