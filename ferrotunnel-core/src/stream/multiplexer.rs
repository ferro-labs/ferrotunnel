//! Stream multiplexer using lock-free data structures
//!
//! Manages multiple virtual streams over a single connection.
//!
//! ## Performance Optimizations (P1)
//! - Larger per-stream channel capacity (128) for better throughput
//! - Reduced backpressure with larger buffers

use super::pool::ObjectPool;
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use ferrotunnel_common::Result;
use ferrotunnel_protocol::frame::{Frame, OpenStreamFrame, Protocol, StreamPriority};
use kanal::{bounded_async, AsyncReceiver, AsyncSender, ReceiveError};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::timeout;
use tracing::warn;

/// Maximum time to wait when sending a frame to the wire channel (`frame_tx`)
/// before treating the peer as unreachable and failing the send.
///
/// The `frame_tx` channel is drained by the batched sender. If that task exits
/// (teardown) or stalls (network partition with a full channel), a plain
/// `send().await` would block forever, preventing the read loop from observing
/// EOF and hanging the session. Bounding every send with this deadline ensures
/// teardown completes promptly. See issue #136.
const FRAME_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Pool for reusing read buffers in `VirtualStream`
pub type ReadBufferPool = ObjectPool<Vec<u8>>;

/// P1.2: Per-stream channel capacity (was 10, now 128 for better throughput)
/// This reduces backpressure and HOL blocking in multiplex scenarios.
const STREAM_CHANNEL_CAPACITY: usize = 128;

/// New stream queue capacity
const NEW_STREAM_QUEUE_CAPACITY: usize = 32;

type CachedSender = (u32, AsyncSender<Result<Frame>>);

/// Channel item for the batched sender: priority (send order) and frame.
pub type PrioritizedFrame = (StreamPriority, Frame);

/// Manages multiple virtual streams over a single connection
///
/// Uses lock-free data structures for high-concurrency performance:
/// - `DashMap` for concurrent stream access without global locks
/// - `AtomicU32` for lock-free stream ID allocation
/// - `kanal` for fast async channel throughput
/// - `ObjectPool` for read buffer reuse
#[derive(Clone, Debug)]
pub struct Multiplexer {
    streams: Arc<DashMap<u32, AsyncSender<Result<Frame>>>>,
    /// Stream priority for send scheduling (cleaned on CloseStream).
    stream_priorities: Arc<DashMap<u32, StreamPriority>>,
    last_sender: Arc<Mutex<Option<CachedSender>>>,
    next_stream_id: Arc<AtomicU32>,
    frame_tx: AsyncSender<PrioritizedFrame>,
    new_stream_tx: AsyncSender<VirtualStream>,
    buffer_pool: ReadBufferPool,
}

/// Send a prioritized frame to the wire channel with a bounded deadline.
///
/// Returns a `BrokenPipe` error if the receiver has been dropped and a
/// `TimedOut` error if the channel stays full past [`FRAME_SEND_TIMEOUT`].
/// Either way the caller fails fast instead of hanging on teardown (#136).
async fn send_prioritized_frame(
    frame_tx: &AsyncSender<PrioritizedFrame>,
    frame: PrioritizedFrame,
) -> Result<()> {
    match timeout(FRAME_SEND_TIMEOUT, frame_tx.send(frame)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()).into()),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out sending frame to wire channel",
        )
        .into()),
    }
}

impl Multiplexer {
    pub fn new(
        frame_tx: AsyncSender<PrioritizedFrame>,
        is_client: bool,
    ) -> (Self, AsyncReceiver<VirtualStream>) {
        let (new_stream_tx, new_stream_rx) = bounded_async(NEW_STREAM_QUEUE_CAPACITY);
        let initial_stream_id = if is_client { 1 } else { 2 };
        (
            Self {
                streams: Arc::new(DashMap::new()),
                stream_priorities: Arc::new(DashMap::new()),
                last_sender: Arc::new(Mutex::new(None)),
                next_stream_id: Arc::new(AtomicU32::new(initial_stream_id)),
                frame_tx,
                new_stream_tx,
                buffer_pool: ReadBufferPool::with_default_capacity(),
            },
            new_stream_rx,
        )
    }

    /// Priority for a frame when sending (used by batched sender order).
    fn priority_for_frame(
        frame: &Frame,
        priorities: &DashMap<u32, StreamPriority>,
    ) -> StreamPriority {
        match frame {
            Frame::Data { stream_id, .. } => priorities
                .get(stream_id)
                .map_or(StreamPriority::Normal, |r| *r),
            Frame::Heartbeat { .. } | Frame::HandshakeAck { .. } => StreamPriority::Critical,
            Frame::CloseStream { stream_id, .. } => priorities
                .get(stream_id)
                .map_or(StreamPriority::Normal, |r| *r),
            _ => StreamPriority::Normal,
        }
    }

    /// Get the buffer pool for reusing read buffers
    pub fn buffer_pool(&self) -> &ReadBufferPool {
        &self.buffer_pool
    }

    /// Allocate a new stream ID atomically (lock-free)
    #[inline]
    fn allocate_stream_id(&self) -> u32 {
        self.next_stream_id.fetch_add(2, Ordering::Relaxed)
    }

    /// Send a frame directly to the wire (priority derived from frame type and stream).
    ///
    /// Bounded by a fixed `FRAME_SEND_TIMEOUT` so a full or torn-down `frame_tx`
    /// channel fails fast instead of blocking the caller forever (issue #136).
    pub async fn send_frame(&self, frame: Frame) -> Result<()> {
        let priority = Self::priority_for_frame(&frame, &self.stream_priorities);
        send_prioritized_frame(&self.frame_tx, (priority, frame)).await
    }

    /// Process an incoming frame from the wire
    ///
    /// P1.2: Uses larger channel capacity (128) to reduce backpressure.
    /// Still uses async send to ensure reliable delivery (dropping data breaks protocols).
    pub async fn process_frame(&self, frame: Frame) -> Result<()> {
        match &frame {
            Frame::OpenStream(open_stream) => {
                let stream_id = open_stream.stream_id;
                let priority = open_stream.priority;
                // P1.2: Use larger channel capacity
                let (tx, rx) = bounded_async(STREAM_CHANNEL_CAPACITY);

                match self.streams.entry(stream_id) {
                    Entry::Occupied(_) => {
                        warn!("Stream {} already exists", stream_id);
                        return Ok(());
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(tx);
                    }
                }
                self.stream_priorities.insert(stream_id, priority);

                let read_buffer = self.buffer_pool.try_acquire().unwrap_or_default();
                let stream = VirtualStream::new(
                    stream_id,
                    rx,
                    self.frame_tx.clone(),
                    priority,
                    read_buffer,
                    self.buffer_pool.clone(),
                    open_stream.protocol,
                );

                // OpenStream is a control path - use async send for reliability
                if self.new_stream_tx.send(stream).await.is_err() {
                    warn!("Failed to queue new stream {}", stream_id);
                    self.streams.remove(&stream_id);
                }
            }
            Frame::Data { stream_id, .. } => {
                let stream_id = *stream_id;
                let tx = self
                    .cached_sender(stream_id)
                    .or_else(|| self.lookup_and_cache_sender(stream_id));

                if let Some(tx) = tx {
                    // Use async send - dropping data breaks protocols (HTTP, etc.)
                    // P1.2: Larger channel (128) reduces chance of blocking
                    if tx.send(Ok(frame)).await.is_err() {
                        self.streams.remove(&stream_id);
                    }
                }
            }
            Frame::CloseStream { stream_id, .. } => {
                let stream_id = *stream_id;
                let tx = self
                    .cached_sender(stream_id)
                    .or_else(|| self.lookup_and_cache_sender(stream_id));

                if let Some(tx) = tx {
                    // Best effort delivery of close frame
                    let _ = tx.send(Ok(frame)).await;
                }
                self.streams.remove(&stream_id);
                self.stream_priorities.remove(&stream_id);
            }
            _ => {}
        }
        Ok(())
    }

    fn cached_sender(&self, stream_id: u32) -> Option<AsyncSender<Result<Frame>>> {
        let guard = self.last_sender.try_lock().ok()?;
        let (cached_id, tx) = guard.as_ref()?;
        if *cached_id == stream_id {
            return Some(tx.clone());
        }
        None
    }

    fn lookup_and_cache_sender(&self, stream_id: u32) -> Option<AsyncSender<Result<Frame>>> {
        let tx = self.streams.get(&stream_id).map(|r| r.clone())?;
        if let Ok(mut guard) = self.last_sender.try_lock() {
            *guard = Some((stream_id, tx.clone()));
        }
        Some(tx)
    }

    /// Open a new outbound stream with default priority.
    pub async fn open_stream(&self, protocol: Protocol) -> Result<VirtualStream> {
        self.open_stream_with_priority(protocol, StreamPriority::default())
            .await
    }

    /// Open a new outbound stream with the given priority (for QoS scheduling).
    pub async fn open_stream_with_priority(
        &self,
        protocol: Protocol,
        priority: StreamPriority,
    ) -> Result<VirtualStream> {
        let stream_id = self.allocate_stream_id();

        let (tx, rx) = bounded_async(STREAM_CHANNEL_CAPACITY);
        self.streams.insert(stream_id, tx);

        self.stream_priorities.insert(stream_id, priority);
        send_prioritized_frame(
            &self.frame_tx,
            (
                priority,
                Frame::OpenStream(Box::new(OpenStreamFrame {
                    stream_id,
                    protocol,
                    headers: vec![],
                    body_hint: None,
                    priority,
                })),
            ),
        )
        .await?;

        let read_buffer = self.buffer_pool.try_acquire().unwrap_or_default();
        Ok(VirtualStream::new(
            stream_id,
            rx,
            self.frame_tx.clone(),
            priority,
            read_buffer,
            self.buffer_pool.clone(),
            protocol,
        ))
    }
}

/// P3.2: Maximum payload size per Frame::Data.
/// Increased to 64KB to reduce framing overhead in throughput tests.
/// Chunking large writes ensures reliable decoding on the wire.
const MAX_DATA_FRAME_PAYLOAD: usize = 64 * 1024; // 64KB

/// Boxed future type for receiving frames
type RecvFuture = Pin<
    Box<dyn std::future::Future<Output = std::result::Result<Result<Frame>, ReceiveError>> + Send>,
>;

/// Boxed future type for sending frames.
///
/// Resolves to an `io::Result` because the send is bounded by
/// [`FRAME_SEND_TIMEOUT`]: a full or closed `frame_tx` surfaces as an error
/// rather than blocking the writer forever (issue #136).
type SendFuture = Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

/// Build the bounded send future stored in `pending_send`.
///
/// `poll_write` / `poll_shutdown` cannot `.await`, so the deadline is baked
/// into the future itself; a timeout or closed channel resolves to an error.
fn bounded_send_future(tx: AsyncSender<PrioritizedFrame>, frame: PrioritizedFrame) -> SendFuture {
    Box::pin(async move {
        match timeout(FRAME_SEND_TIMEOUT, tx.send(frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(io::Error::new(io::ErrorKind::BrokenPipe, e.to_string())),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out sending frame to wire channel",
            )),
        }
    })
}

/// A virtual stream that implements `AsyncRead` + `AsyncWrite`
///
/// Uses kanal channels for async communication.
/// The polling implementation uses boxed futures to bridge kanal's
/// async API with tokio's poll-based traits.
///
/// Read buffers are pooled and returned when the stream is dropped.
///
/// ## Performance Optimizations (P3)
/// - P3.1: Store `pending_send_len` instead of cloning the frame
/// - P3.3: Use `read_buffer_pos` cursor instead of drain() for O(1) reads
pub struct VirtualStream {
    stream_id: u32,
    rx: AsyncReceiver<Result<Frame>>,
    tx: AsyncSender<PrioritizedFrame>,
    priority: StreamPriority,
    read_buffer: Vec<u8>,
    read_buffer_bytes: Option<Bytes>,
    /// P3.3: Cursor position in read_buffer (avoids O(n) drain)
    read_buffer_pos: usize,
    /// Pool to return `read_buffer` to when dropped
    buffer_pool: Option<ReadBufferPool>,
    /// Pending receive future for `poll_read`
    pending_recv: Option<RecvFuture>,
    /// Pending send future for `poll_write`
    pending_send: Option<SendFuture>,
    /// P3.1: Store bytes written instead of cloning frame
    pending_send_len: usize,
    /// Protocol for this stream
    protocol: Protocol,
}

impl std::fmt::Debug for VirtualStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualStream")
            .field("stream_id", &self.stream_id)
            .field("read_buffer_len", &self.read_buffer.len())
            .finish_non_exhaustive()
    }
}

impl VirtualStream {
    /// Create a new `VirtualStream` with a pooled read buffer.
    pub fn new(
        stream_id: u32,
        rx: AsyncReceiver<Result<Frame>>,
        tx: AsyncSender<PrioritizedFrame>,
        priority: StreamPriority,
        read_buffer: Vec<u8>,
        buffer_pool: ReadBufferPool,
        protocol: Protocol,
    ) -> Self {
        Self {
            stream_id,
            rx,
            tx,
            priority,
            read_buffer,
            read_buffer_bytes: None,
            read_buffer_pos: 0,
            buffer_pool: Some(buffer_pool),
            pending_recv: None,
            pending_send: None,
            pending_send_len: 0,
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

impl Drop for VirtualStream {
    fn drop(&mut self) {
        // Return the read buffer to the pool if available
        if let Some(pool) = self.buffer_pool.take() {
            let buffer = std::mem::take(&mut self.read_buffer);
            pool.release(buffer);
        }
    }
}

impl AsyncRead for VirtualStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(buffered_bytes) = self.read_buffer_bytes.as_mut() {
            if !buffered_bytes.is_empty() {
                let len = std::cmp::min(buf.remaining(), buffered_bytes.len());
                let chunk = buffered_bytes.split_to(len);
                buf.put_slice(&chunk);
                if buffered_bytes.is_empty() {
                    self.read_buffer_bytes = None;
                }
                return Poll::Ready(Ok(()));
            }
            self.read_buffer_bytes = None;
        }

        // P3.3: First, return any buffered data using cursor (O(1) instead of O(n) drain)
        let buffered_remaining = self.read_buffer.len() - self.read_buffer_pos;
        if buffered_remaining > 0 {
            let len = std::cmp::min(buf.remaining(), buffered_remaining);
            let start = self.read_buffer_pos;
            buf.put_slice(&self.read_buffer[start..start + len]);
            self.read_buffer_pos += len;

            // If we've consumed all buffered data, reset the buffer
            if self.read_buffer_pos >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Check if we have a pending receive future
        if self.pending_recv.is_none() {
            let rx = self.rx.clone();
            self.pending_recv = Some(Box::pin(async move { rx.recv().await }));
        }

        // Poll the pending future (we set it in the block above)
        let fut = match self.pending_recv.as_mut() {
            Some(f) => f,
            None => return Poll::Pending,
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_recv = None;
                match result {
                    Ok(Ok(Frame::Data {
                        data: bytes,
                        end_of_stream: _,
                        ..
                    })) => {
                        let len = std::cmp::min(buf.remaining(), bytes.len());
                        buf.put_slice(&bytes[..len]);
                        if len < bytes.len() {
                            self.read_buffer_bytes = Some(bytes.slice(len..));
                        }
                        Poll::Ready(Ok(()))
                    }
                    Ok(Ok(Frame::CloseStream { .. })) | Err(ReceiveError::Closed) => {
                        Poll::Ready(Ok(())) // EOF
                    }
                    Ok(Ok(_)) => Poll::Pending, // Ignore other frame types
                    Ok(Err(e)) => Poll::Ready(Err(io::Error::other(e.to_string()))),
                    Err(ReceiveError::SendClosed) => Poll::Ready(Ok(())), // EOF
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for VirtualStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // If we have a pending send, poll it first
        if let Some(fut) = self.pending_send.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    // P3.1: Use stored length instead of cloning frame
                    let bytes_written = self.pending_send_len;
                    self.pending_send = None;
                    self.pending_send_len = 0;
                    return Poll::Ready(result.map(|()| bytes_written));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Chunk large writes to stay within protocol limits
        let chunk_size = buf.len().min(MAX_DATA_FRAME_PAYLOAD);

        // Zero-copy: use Bytes::copy_from_slice for optimal performance
        // This is still a copy, but avoids BytesMut allocation overhead
        let data = Bytes::copy_from_slice(&buf[..chunk_size]);

        // P3.1: Build frame directly for sending (no clone needed)
        let frame = Frame::Data {
            stream_id: self.stream_id,
            data,
            end_of_stream: false,
        };
        let priority = self.priority;

        let tx = self.tx.clone();
        // P3.1: Store length instead of frame, move frame into future
        self.pending_send_len = chunk_size;
        self.pending_send = Some(bounded_send_future(tx, (priority, frame)));

        // Poll the new future (we set it in the block above)
        let fut = match self.pending_send.as_mut() {
            Some(f) => f,
            None => return Poll::Pending,
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_send = None;
                self.pending_send_len = 0;
                Poll::Ready(result.map(|()| chunk_size))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Kanal channels don't require explicit flushing
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // If we have a pending send, poll it first
        if let Some(fut) = self.pending_send.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    self.pending_send = None;
                    self.pending_send_len = 0;
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Send close frame
        let frame = Frame::CloseStream {
            stream_id: self.stream_id,
            reason: ferrotunnel_protocol::frame::CloseReason::Normal,
        };
        let priority = self.priority;

        let tx = self.tx.clone();
        self.pending_send = Some(bounded_send_future(tx, (priority, frame)));

        let fut = match self.pending_send.as_mut() {
            Some(f) => f,
            None => return Poll::Pending,
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_send = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stream_id_allocation() {
        let (tx, _rx) = bounded_async(100);

        // Client (Odd IDs)
        let (client_mux, _client_streams) = Multiplexer::new(tx.clone(), true);

        let s1 = client_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s1.id(), 1);

        let s2 = client_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s2.id(), 3);

        // Server (Even IDs)
        let (server_mux, _server_streams) = Multiplexer::new(tx, false);

        let s3 = server_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s3.id(), 2);

        let s4 = server_mux.open_stream(Protocol::HTTP).await.unwrap();
        assert_eq!(s4.id(), 4);
    }

    /// Issue #136: a send must fail fast when the receiver is gone, instead of
    /// blocking the session loop forever during teardown.
    #[tokio::test]
    async fn send_frame_fails_fast_when_receiver_dropped() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(1);
        drop(rx);
        let (mux, _streams) = Multiplexer::new(tx, true);

        let result = mux.send_frame(Frame::Heartbeat { timestamp: 0 }).await;

        assert!(
            result.is_err(),
            "send to a closed frame_tx must return an error"
        );
    }

    /// Issue #136: a send must time out (not hang) when the channel stays full
    /// because the batched sender stopped draining (e.g. network partition).
    ///
    /// `start_paused` lets tokio auto-advance virtual time to the send timeout,
    /// so the test asserts fail-fast behavior without waiting wall-clock.
    #[tokio::test(start_paused = true)]
    async fn send_frame_times_out_when_channel_full() {
        let (tx, _rx) = bounded_async::<PrioritizedFrame>(1);
        let (mux, _streams) = Multiplexer::new(tx, true);

        // Fill the single slot; `_rx` is kept alive but never drained.
        mux.send_frame(Frame::Heartbeat { timestamp: 0 })
            .await
            .expect("first send fills the only slot");

        let result = mux.send_frame(Frame::Heartbeat { timestamp: 1 }).await;

        assert!(
            result.is_err(),
            "send to a full frame_tx must time out rather than hang"
        );
    }

    /// Issue #136: `VirtualStream::poll_write` must not block forever when the
    /// wire channel is full; the bounded send surfaces an error instead.
    #[tokio::test(start_paused = true)]
    async fn virtual_stream_write_times_out_when_channel_full() {
        use tokio::io::AsyncWriteExt;

        let (tx, _rx) = bounded_async::<PrioritizedFrame>(1);
        let (mux, _streams) = Multiplexer::new(tx, true);

        // `open_stream` consumes the only channel slot with its OpenStream frame.
        let mut stream = mux
            .open_stream(Protocol::HTTP)
            .await
            .expect("open_stream uses the only slot");

        // Channel is now full and never drained: the write must fail fast.
        let result = stream.write(b"hello").await;

        assert!(
            result.is_err(),
            "write to a full frame_tx must time out rather than hang"
        );
    }
}
