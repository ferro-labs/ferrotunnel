pub mod bytes_pool;
pub mod multiplexer;
pub mod pool;
#[cfg(feature = "quic")]
pub mod quic_multiplexer;

pub use multiplexer::{Multiplexer, PrioritizedFrame, VirtualStream};
pub use pool::{ByteBufferPool, ObjectPool, Poolable, PooledObject};
#[cfg(feature = "quic")]
pub use quic_multiplexer::{QuicMultiplexer, QuicVirtualStream};

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
    pub async fn open_stream(&self, protocol: Protocol) -> Result<BoxedStream> {
        match self {
            Self::Tcp(m) => {
                let stream = m.open_stream(protocol).await?;
                Ok(Box::pin(stream))
            }
            #[cfg(feature = "quic")]
            Self::Quic(m) => {
                let stream = m.open_stream(protocol).await?;
                Ok(Box::pin(stream))
            }
        }
    }
}
