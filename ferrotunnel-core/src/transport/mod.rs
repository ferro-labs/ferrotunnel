//! Transport layer abstraction for TCP and TLS
//!
//! For a transport-agnostic frame API (QUIC/HTTP2 ready), see [`FrameSender`] and [`FrameReceiver`].

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

pub mod batched_sender;
pub mod frame_transport;
#[cfg(feature = "quic")]
pub mod quic;
pub mod socket_tuning;
pub mod tcp;
pub mod tcp_frame;
pub mod tls;

pub use frame_transport::{FrameConnectionSplit, FrameReceiver, FrameSender};
pub use tcp_frame::{TcpFrameReceiver, TcpFrameSender};

pub trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncStream for T {}

pub type BoxedStream = Pin<Box<dyn AsyncStream>>;

/// Transport selection for the control connection.
///
/// Future versions may extend this with `Quic(...)` (v0.4) or `Http2(...)` (v0.2);
/// the same [`Frame`](ferrotunnel_protocol::Frame) protocol can run over any variant
/// via [`FrameSender`] / [`FrameReceiver`].
#[derive(Debug, Clone, Default)]
pub enum TransportConfig {
    #[default]
    Tcp,
    Tls(tls::TlsTransportConfig),
    #[cfg(feature = "quic")]
    Quic(quic::QuicTransportConfig),
}

/// Connect to a server using the configured transport (TCP or TLS).
///
/// QUIC connections do not use this function — they use [`quic::connect`] directly
/// because QUIC operates over UDP, not TCP.
pub async fn connect(config: &TransportConfig, addr: &str) -> io::Result<BoxedStream> {
    match config {
        TransportConfig::Tcp => tcp::connect(addr).await,
        TransportConfig::Tls(tls_config) => tls::connect(addr, tls_config).await,
        #[cfg(feature = "quic")]
        TransportConfig::Quic(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "QUIC transport uses quic::connect() directly, not this function",
        )),
    }
}

/// Accept a connection from the configured transport (TCP or TLS).
///
/// QUIC connections do not use this function — they use [`quic::accept`] directly
/// because QUIC operates over UDP endpoints, not TCP listeners.
pub async fn accept(
    config: &TransportConfig,
    listener: &TcpListener,
) -> io::Result<(BoxedStream, SocketAddr)> {
    let (tcp_stream, addr) = listener.accept().await?;
    socket_tuning::configure_socket_silent(&tcp_stream);

    match config {
        TransportConfig::Tcp => Ok((Box::pin(tcp_stream), addr)),
        TransportConfig::Tls(tls_config) => {
            let tls_stream = tls::accept_tls(tcp_stream, tls_config).await?;
            Ok((Box::pin(tls_stream), addr))
        }
        #[cfg(feature = "quic")]
        TransportConfig::Quic(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "QUIC transport uses quic::accept() directly, not this function",
        )),
    }
}
