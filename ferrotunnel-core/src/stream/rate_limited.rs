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
/// Over-budget data is delayed rather than dropped: bytes are read into an
/// internal buffer and withheld until the session has paid for them, so quota
/// is always reserved *before* the caller can see them. Withholding is what
/// makes the cap hold for short-lived streams — the ingress opens a fresh
/// stream per request, so a wrapper that released bytes first and settled up
/// on the next read would let every request through unmetered and be dropped
/// before the debt came due. Not reading propagates backpressure through QUIC
/// flow control to the peer.
pub struct RateLimitedStream<S> {
    inner: S,
    limiter: SessionRateLimiter,
    throttle: Option<ThrottleFuture>,
    /// Bytes read from `inner` but not yet released to the caller.
    pending: Vec<u8>,
    /// How much of `pending` has already been handed over.
    released: usize,
}

impl<S> RateLimitedStream<S> {
    /// Meter `inner` against `limiter`.
    pub fn new(inner: S, limiter: SessionRateLimiter) -> Self {
        Self {
            inner,
            limiter,
            throttle: None,
            pending: Vec::new(),
            released: 0,
        }
    }

    /// Hand buffered bytes to the caller, returning true if any were released.
    fn drain_pending(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        let available = &self.pending[self.released..];
        if available.is_empty() {
            return false;
        }
        let take = available.len().min(buf.remaining());
        buf.put_slice(&available[..take]);
        self.released += take;
        if self.released == self.pending.len() {
            self.pending.clear();
            self.released = 0;
        }
        take > 0
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

        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // Settle any outstanding debt first. Leaving the inner read unpolled is
        // what applies backpressure to the peer.
        if let Some(throttle) = this.throttle.as_mut() {
            match throttle.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => this.throttle = None,
            }
        }

        // Paid-for bytes from an earlier read, or a remainder that did not fit
        // the caller's buffer.
        if this.drain_pending(buf) {
            return Poll::Ready(Ok(()));
        }

        // Read into our own buffer so the bytes can be withheld until paid for.
        let capacity = buf.remaining();
        this.pending.resize(capacity, 0);
        this.released = 0;
        let mut staged = ReadBuf::new(&mut this.pending);
        match Pin::new(&mut this.inner).poll_read(cx, &mut staged) {
            Poll::Pending => {
                this.pending.clear();
                return Poll::Pending;
            }
            Poll::Ready(Err(e)) => {
                this.pending.clear();
                return Poll::Ready(Err(e));
            }
            Poll::Ready(Ok(())) => {}
        }
        let read = staged.filled().len();
        this.pending.truncate(read);

        // EOF: nothing to meter, and the empty fill signals it to the caller.
        if read == 0 {
            return Poll::Ready(Ok(()));
        }

        // `check_data` consumes quota when the payload fits and consumes
        // nothing when it does not, so the failing branch can hand the same
        // byte count to `throttle_data` without double-charging the session.
        if this.limiter.check_data(read).is_ok() {
            this.drain_pending(buf);
            return Poll::Ready(Ok(()));
        }

        let limiter = this.limiter.clone();
        // No heartbeat callback is needed here, unlike the TCP path: QUIC
        // carries heartbeats on a dedicated control stream, so a throttled
        // data stream cannot stall them into a stale-session eviction.
        let mut throttle: ThrottleFuture = Box::pin(async move {
            limiter.throttle_data(read, || {}).await;
        });
        match throttle.as_mut().poll(cx) {
            // Quota was already available; release without a round trip.
            Poll::Ready(()) => {
                this.drain_pending(buf);
                Poll::Ready(Ok(()))
            }
            // Bytes stay in `pending` until the debt clears.
            Poll::Pending => {
                this.throttle = Some(throttle);
                Poll::Pending
            }
        }
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
    async fn over_budget_reads_are_delayed_without_losing_data() {
        // 512 B/s with no burst headroom: the first read empties the bucket and
        // the second cannot be served until quota refills.
        let (mut peer, inner) = duplex(4096);
        peer.write_all(&[7u8; 1024]).await.unwrap();

        let mut stream = RateLimitedStream::new(inner, limiter(512, 1));
        let mut chunks = [vec![0u8; 512], vec![0u8; 512]];

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
    async fn a_single_over_budget_read_is_paid_for_before_delivery() {
        // The case a release-then-settle-up design cannot cap, and the shape
        // the ingress actually produces: read exactly the expected body, then
        // drop the stream. There is no later poll to collect the debt on, so
        // the bytes must be paid for before they are handed over or the cap
        // does not hold at all.
        let (mut peer, inner) = duplex(8192);
        peer.write_all(&[9u8; 1024]).await.unwrap();

        // 512 B/s, burst 1x: a 1 KiB read is twice the whole bucket.
        let mut stream = RateLimitedStream::new(inner, limiter(512, 1));
        let mut body = vec![0u8; 1024];

        let start = Instant::now();
        stream.read_exact(&mut body).await.unwrap();
        let elapsed = start.elapsed();
        drop(stream);

        assert!(body.iter().all(|&b| b == 9), "delivered data was corrupted");
        assert!(
            elapsed >= Duration::from_millis(500),
            "an oversized read must be paid for before its bytes are released, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn withheld_bytes_survive_a_caller_buffer_smaller_than_the_read() {
        // The remainder of a staged read must be handed over on later polls
        // rather than dropped when it does not fit the caller's buffer.
        let (mut peer, inner) = duplex(4096);
        peer.write_all(&[4u8; 256]).await.unwrap();
        drop(peer);

        let mut stream = RateLimitedStream::new(inner, limiter(1024 * 1024, 2));
        let mut sink = Vec::new();
        let mut chunk = [0u8; 16];

        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            sink.extend_from_slice(&chunk[..n]);
        }

        assert_eq!(sink.len(), 256, "every byte must be delivered exactly once");
        assert!(sink.iter().all(|&b| b == 4), "delivered data was corrupted");
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
