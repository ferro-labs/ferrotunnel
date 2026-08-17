pub mod bytes_pool;
pub mod multiplexer;
pub mod pool;
#[cfg(feature = "quic")]
pub mod quic_multiplexer;
#[cfg(feature = "quic")]
pub mod rate_limited;

pub use multiplexer::{Multiplexer, PrioritizedFrame, VirtualStream};
pub use pool::{ByteBufferPool, ObjectPool, Poolable, PooledObject};
#[cfg(feature = "quic")]
pub use quic_multiplexer::{QuicMultiplexer, QuicVirtualStream};
#[cfg(feature = "quic")]
pub use rate_limited::RateLimitedStream;

use super::transport::BoxedStream;
use ferrotunnel_common::Result;
use ferrotunnel_protocol::frame::Protocol;

/// Unified multiplexer that can open streams over TCP or QUIC transport.
///
/// The HTTP ingress uses this to open streams without knowing the underlying transport.
#[derive(Clone, Debug)]
pub enum AnyMultiplexer {
    Tcp(Multiplexer),
    #[cfg(feature = "quic")]
    Quic(QuicMultiplexer),
}

impl AnyMultiplexer {
    /// Open a new outbound stream, returning a boxed `AsyncRead + AsyncWrite`.
    ///
    /// On QUIC this is also where per-session rate limits are applied, because
    /// QUIC has no equivalent of the TCP frame loop that
    /// `TunnelServer::process_messages` meters. Stream opens are checked here
    /// and the returned stream carries the session's byte budget; see
    /// [`RateLimitedStream`]. The TCP arm is left alone, since its traffic is
    /// already metered as it passes through the frame loop.
    ///
    /// # Errors
    ///
    /// Returns [`ferrotunnel_common::TunnelError::ServiceUnavailable`] when the
    /// session's stream-open rate has been exceeded.
    pub async fn open_stream(&self, protocol: Protocol) -> Result<BoxedStream> {
        match self {
            Self::Tcp(m) => {
                let stream = m.open_stream(protocol).await?;
                Ok(Box::pin(stream))
            }
            #[cfg(feature = "quic")]
            Self::Quic(m) => {
                let Some(limiter) = m.rate_limiter() else {
                    let stream = m.open_stream(protocol).await?;
                    return Ok(Box::pin(stream));
                };
                limiter.check_stream_open()?;
                let limiter = limiter.clone();
                let stream = m.open_stream(protocol).await?;
                Ok(Box::pin(RateLimitedStream::new(stream, limiter)))
            }
        }
    }
}
