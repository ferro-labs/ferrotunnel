//! Per-session byte-rate enforcement for transports with no central frame loop.
//!
//! The TCP transport multiplexes every stream onto one connection, so
//! `TunnelServer::process_messages` sees each inbound `Frame::Data` and applies
//! the session budget there. QUIC maps each stream to its own connection-level
//! stream, so no such choke point exists: bytes travel straight between the
//! ingress and `quinn`. This adapter restores the budget by applying it to the
//! stream itself.

use crate::rate_limit::SessionRateLimiter;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

type ThrottleFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Applies a session's byte budget to the inbound half of a stream.
///
/// Only reads are metered. The budget is defined over client-to-server traffic,
/// which is what `process_messages` meters on the TCP path, so metering writes
/// as well would make the same setting mean different things per transport.
///
/// Over-budget data is delayed rather than dropped, matching the TCP path: the
/// bytes already read are delivered, and the *next* read waits for quota. Not
/// reading propagates backpressure through QUIC flow control to the peer.
pub struct RateLimitedStream<S> {
    inner: S,
    limiter: SessionRateLimiter,
    throttle: Option<ThrottleFuture>,
}

impl<S> RateLimitedStream<S> {
    /// Meter `inner` against `limiter`.
    pub fn new(inner: S, limiter: SessionRateLimiter) -> Self {
        Self {
            inner,
            limiter,
            throttle: None,
        }
    }
}

impl<S: std::fmt::Debug> std::fmt::Debug for RateLimitedStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitedStream")
            .field("inner", &self.inner)
            .field("throttled", &self.throttle.is_some())
            .finish_non_exhaustive()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for RateLimitedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Serve an outstanding throttle before pulling more data. Leaving the
        // read unpolled is what applies backpressure to the peer.
        if let Some(throttle) = this.throttle.as_mut() {
            match throttle.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => this.throttle = None,
            }
        }

        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if !matches!(result, Poll::Ready(Ok(()))) {
            return result;
        }

        let read = buf.filled().len() - before;
        // `check_data` consumes quota when the payload fits and consumes
        // nothing when it does not, so the failing branch can hand the same
        // byte count to `throttle_data` without double-charging the session.
        if read > 0 && this.limiter.check_data(read).is_err() {
            let limiter = this.limiter.clone();
            // No heartbeat callback is needed here, unlike the TCP path: QUIC
            // carries heartbeats on a dedicated control stream, so a throttled
            // data stream cannot stall them into a stale-session eviction.
            this.throttle = Some(Box::pin(async move {
                limiter.throttle_data(read, || {}).await;
            }));
        }

        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for RateLimitedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::RateLimiterConfig;
    use std::num::NonZeroU32;
    use std::time::{Duration, Instant};
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn limiter(bytes_per_sec: u32, burst_factor: u32) -> SessionRateLimiter {
        SessionRateLimiter::new(&RateLimiterConfig {
            streams_per_sec: NonZeroU32::new(1000).unwrap(),
            bytes_per_sec: NonZeroU32::new(bytes_per_sec).unwrap(),
            burst_factor: NonZeroU32::new(burst_factor).unwrap(),
        })
    }

    #[tokio::test]
    async fn reads_within_budget_are_not_delayed() {
        let (mut peer, inner) = duplex(4096);
        peer.write_all(&[0u8; 256]).await.unwrap();

        let mut stream = RateLimitedStream::new(inner, limiter(1_000_000, 2));
        let mut buf = vec![0u8; 256];

        let start = Instant::now();
        stream.read_exact(&mut buf).await.unwrap();

        assert!(
            start.elapsed() < Duration::from_millis(100),
            "a read inside the budget must not be throttled"
        );
    }

    #[tokio::test]
    async fn over_budget_reads_delay_the_next_read_without_losing_data() {
        // 512 B/s with no burst headroom. Charging happens after delivery, so
        // it takes three reads to observe the delay: the first drains the
        // bucket, the second goes over and arms the throttle, and the third
        // pays for it by waiting for quota to refill.
        let (mut peer, inner) = duplex(4096);
        peer.write_all(&[7u8; 1536]).await.unwrap();

        let mut stream = RateLimitedStream::new(inner, limiter(512, 1));
        let mut chunks = [vec![0u8; 512], vec![0u8; 512], vec![0u8; 512]];

        let start = Instant::now();
        for chunk in &mut chunks {
            stream.read_exact(chunk).await.unwrap();
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(500),
            "an over-budget stream must be throttled, took {elapsed:?}"
        );
        // Throttling must delay bytes, never drop or reorder them.
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.iter().all(|&b| b == 7),
                "read {i} returned corrupted data"
            );
        }
    }

    #[tokio::test]
    async fn a_stream_inside_its_budget_runs_to_completion_undelayed() {
        // Guards the throttle from firing on ordinary traffic: 1 MiB/s of
        // budget against 4 KiB of data must never arm it.
        let (mut peer, inner) = duplex(8192);
        peer.write_all(&[3u8; 4096]).await.unwrap();
        drop(peer);

        let mut stream = RateLimitedStream::new(inner, limiter(1024 * 1024, 2));
        let mut sink = Vec::new();

        let start = Instant::now();
        stream.read_to_end(&mut sink).await.unwrap();

        assert_eq!(sink.len(), 4096, "every byte must be delivered");
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "traffic inside the budget must not be delayed"
        );
    }

    #[tokio::test]
    async fn writes_are_not_metered() {
        let (mut peer, inner) = duplex(4096);
        let mut stream = RateLimitedStream::new(inner, limiter(512, 1));

        let start = Instant::now();
        stream.write_all(&[1u8; 2048]).await.unwrap();
        stream.flush().await.unwrap();

        assert!(
            start.elapsed() < Duration::from_millis(100),
            "the outbound half carries server-to-client traffic and is not metered"
        );

        let mut echo = vec![0u8; 2048];
        peer.read_exact(&mut echo).await.unwrap();
    }
}
