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
    /// This is where the ingress's own stream opens are metered. The TCP frame
    /// loop in `TunnelServer::process_messages` only sees `OpenStream` frames
    /// sent *by the tunnel client*, so without a check here `streams_per_sec`
    /// would not apply to the streams the ingress opens on either transport.
    ///
    /// Byte rates are handled per transport. TCP traffic is already metered as
    /// it passes through the frame loop, so its streams are returned unwrapped.
    /// QUIC has no such choke point, so its streams carry the session's byte
    /// budget via [`RateLimitedStream`].
    ///
    /// # Errors
    ///
    /// Returns [`ferrotunnel_common::TunnelError::ServiceUnavailable`] when the
    /// session's stream-open rate has been exceeded.
    pub async fn open_stream(&self, protocol: Protocol) -> Result<BoxedStream> {
        if let Some(limiter) = self.rate_limiter() {
            limiter.check_stream_open()?;
        }

        match self {
            Self::Tcp(m) => {
                let stream = m.open_stream(protocol).await?;
                Ok(Box::pin(stream))
            }
            #[cfg(feature = "quic")]
            Self::Quic(m) => {
                let stream = m.open_stream(protocol).await?;
                match m.rate_limiter() {
                    Some(limiter) => Ok(Box::pin(RateLimitedStream::new(stream, limiter.clone()))),
                    None => Ok(Box::pin(stream)),
                }
            }
        }
    }

    /// The session rate limiter behind this multiplexer, if one was attached.
    fn rate_limiter(&self) -> Option<&crate::rate_limit::SessionRateLimiter> {
        match self {
            Self::Tcp(m) => m.rate_limiter(),
            #[cfg(feature = "quic")]
            Self::Quic(m) => m.rate_limiter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::{RateLimiterConfig, SessionRateLimiter};
    use kanal::bounded_async;
    use std::num::NonZeroU32;

    #[allow(clippy::unwrap_used)]
    fn one_stream_per_second() -> SessionRateLimiter {
        SessionRateLimiter::new(&RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(1).unwrap(),
            bytes_per_sec: NonZeroU32::new(1_000_000).unwrap(),
            burst_factor: NonZeroU32::new(1).unwrap(),
        })
    }

    #[tokio::test]
    async fn tcp_ingress_stream_opens_are_rate_limited() {
        // `process_messages` only meters `OpenStream` frames sent by the
        // client, so without the check in `open_stream` the ingress could open
        // unlimited streams on a TCP session.
        let (frame_tx, _frame_rx) = bounded_async::<PrioritizedFrame>(100);
        let (mux, _accept) = Multiplexer::new(frame_tx, true);
        let mux = AnyMultiplexer::Tcp(mux.with_rate_limiter(one_stream_per_second()));

        mux.open_stream(Protocol::TCP)
            .await
            .expect("the first open is within budget");

        // `BoxedStream` is not `Debug`, so unwrap the error by hand.
        let Err(err) = mux.open_stream(Protocol::TCP).await else {
            panic!("a second open in the same second must exceed the budget")
        };
        assert!(
            err.to_string().contains("rate limited"),
            "error should report the rate limit: {err}"
        );
    }

    #[tokio::test]
    async fn tcp_streams_open_freely_without_a_limiter() {
        let (frame_tx, _frame_rx) = bounded_async::<PrioritizedFrame>(100);
        let (mux, _accept) = Multiplexer::new(frame_tx, true);
        let mux = AnyMultiplexer::Tcp(mux);

        for _ in 0..8 {
            mux.open_stream(Protocol::TCP)
                .await
                .expect("an unmetered multiplexer must not throttle opens");
        }
    }
}
