//! Batched frame sender for reduced syscall overhead
//!
//! Collects multiple frames and flushes them together. Uses length-prefixed
//! framing and vectored writes to avoid extra payload copies.
//!
//! ## Performance Optimizations (P2)
//! - Adaptive batching: immediate flush when idle (single frame, low load)
//! - Only batch when under sustained load (reduces latency for interactive use)
//! - Removed unnecessary flush() for raw TCP (TCP_NODELAY handles it)

use crate::stream::PrioritizedFrame;
use bytes::{BufMut, Bytes, BytesMut};
use ferrotunnel_protocol::codec::TunnelCodec;
use ferrotunnel_protocol::Frame;
use kanal::AsyncReceiver;
use std::io;
use std::io::IoSlice;
#[cfg(feature = "metrics")]
use std::time::Instant;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Encoder;
use tracing::warn;

/// Maximum frames to batch before flushing
const MAX_BATCH_SIZE: usize = 256;

/// Minimum frames before we consider waiting for more
/// If we have fewer frames, flush immediately for lower latency
const MIN_FRAMES_FOR_BATCHING: usize = 2;

/// Spawns a batched sender task that collects frames and flushes them together.
/// Frames are drained in priority order (Critical → High → Normal → Low).
///
/// With length-prefixed framing, each frame is encoded with a header. Data
/// frames use vectored writes to avoid copying payload bytes.
///
/// ## P2 Batching Strategy
/// - Always try to batch frames for throughput
/// - A single scheduler yield while batching balances latency vs throughput
/// - Single frame: flush immediately (no wait)
pub async fn run_batched_sender<W>(
    frame_rx: AsyncReceiver<PrioritizedFrame>,
    mut writer: W,
    mut codec: TunnelCodec,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut frames = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut encoded_segments = Vec::with_capacity(MAX_BATCH_SIZE * 2);

    loop {
        frames.clear();
        encoded_segments.clear();

        // Wait for first frame
        if let Ok(pf) = frame_rx.recv().await {
            frames.push(pf);
        } else {
            break;
        }

        // Try to collect more frames without blocking (non-blocking drain)
        while frames.len() < MAX_BATCH_SIZE {
            match frame_rx.try_recv() {
                Ok(Some(pf)) => frames.push(pf),
                Ok(None) | Err(_) => break,
            }
        }

        // If we got multiple frames, yield once so in-flight producers can enqueue
        // This improves throughput under load while keeping latency low
        if frames.len() >= MIN_FRAMES_FOR_BATCHING && frames.len() < MAX_BATCH_SIZE {
            tokio::task::yield_now().await;
            while frames.len() < MAX_BATCH_SIZE {
                match frame_rx.try_recv() {
                    Ok(Some(pf)) => frames.push(pf),
                    Ok(None) | Err(_) => break,
                }
            }
        }

        // Send in priority order: Critical first, then High, Normal, Low
        frames.sort_by_key(|(p, _)| p.drain_order());

        #[cfg(feature = "metrics")]
        let n_frames = frames.len();

        #[cfg(feature = "metrics")]
        if let Some(m) = ferrotunnel_observability::tunnel_metrics() {
            m.set_queue_depth(n_frames);
        }

        #[cfg(feature = "metrics")]
        let encode_start = Instant::now();
        // Encode all frames using vectored writes for zero-copy data frames
        for (_priority, frame) in frames.drain(..) {
            if let Err(e) = encode_frame_segments(&mut codec, frame, &mut encoded_segments) {
                warn!("Skipping invalid frame: {}", e);
            }
        }
        #[cfg(feature = "metrics")]
        let encoded_bytes: usize = encoded_segments.iter().map(Bytes::len).sum();

        #[cfg(feature = "metrics")]
        if let Some(m) = ferrotunnel_observability::tunnel_metrics() {
            m.record_encode(n_frames, encoded_bytes, encode_start.elapsed());
        }

        // Write to socket using vectored I/O
        if !encoded_segments.is_empty() {
            if let Err(e) = write_all_vectored(&mut writer, &encoded_segments).await {
                warn!("Failed to write batched frames: {}", e);
                break;
            }
        }
    }
}

const FRAME_TYPE_DATA: u8 = 0x01;
const FLAG_EOS: u8 = 0x01;

fn encode_frame_segments(
    codec: &mut TunnelCodec,
    frame: Frame,
    out: &mut Vec<Bytes>,
) -> io::Result<()> {
    match frame {
        Frame::Data {
            stream_id,
            data,
            end_of_stream,
        } => {
            let payload_len = 1 + 4 + 1 + data.len();
            if payload_len > codec.max_frame_size() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Frame too large: {} bytes (max: {})",
                        payload_len,
                        codec.max_frame_size()
                    ),
                ));
            }

            let mut header = BytesMut::with_capacity(4 + 1 + 4 + 1);
            header.put_u32(payload_len as u32);
            header.put_u8(FRAME_TYPE_DATA);
            header.put_u32(stream_id);
            header.put_u8(if end_of_stream { FLAG_EOS } else { 0 });
            out.push(header.freeze());

            if !data.is_empty() {
                out.push(data);
            }
        }
        control_frame => {
            let mut buf = BytesMut::new();
            codec.encode(control_frame, &mut buf)?;
            out.push(buf.freeze());
        }
    }

    Ok(())
}

async fn write_all_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buffers: &[Bytes],
) -> io::Result<()> {
    let mut index = 0;
    let mut offset = 0;

    while index < buffers.len() {
        while index < buffers.len() && buffers[index].is_empty() {
            index += 1;
            offset = 0;
        }
        if index >= buffers.len() {
            break;
        }

        let mut slices = Vec::with_capacity(buffers.len() - index);
        slices.push(IoSlice::new(&buffers[index][offset..]));
        for buf in &buffers[(index + 1)..] {
            if !buf.is_empty() {
                slices.push(IoSlice::new(buf));
            }
        }

        if slices.is_empty() {
            break;
        }

        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "Failed to write buffered frames",
            ));
        }

        let mut remaining = written;
        while remaining > 0 && index < buffers.len() {
            let available = buffers[index].len().saturating_sub(offset);
            if remaining < available {
                offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                index += 1;
                offset = 0;
                while index < buffers.len() && buffers[index].is_empty() {
                    index += 1;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ferrotunnel_protocol::codec::TunnelCodec;
    use ferrotunnel_protocol::frame::StreamPriority;
    use kanal::bounded_async;
    use std::time::{Duration, Instant};
    use tokio::io::duplex;
    use tokio::io::AsyncReadExt;
    use tokio::time::timeout;

    fn pf(priority: StreamPriority, frame: Frame) -> PrioritizedFrame {
        (priority, frame)
    }

    #[tokio::test]
    async fn test_batched_sender_single_frame() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(10);
        let (writer, mut reader) = duplex(8192);

        tokio::spawn(async move {
            run_batched_sender(rx, writer, TunnelCodec::new()).await;
        });

        tx.send(pf(
            StreamPriority::Normal,
            Frame::Heartbeat { timestamp: 123 },
        ))
        .await
        .unwrap();
        drop(tx);

        // Verify we can read something (basic check)
        let mut buf = [0u8; 100];
        let n = reader.read(&mut buf).await.unwrap();
        assert!(n > 0);
    }

    #[tokio::test]
    async fn test_batched_sender_multiple_frames() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(10);
        let (writer, mut reader) = duplex(8192);

        tokio::spawn(async move {
            run_batched_sender(rx, writer, TunnelCodec::new()).await;
        });

        for i in 0..5 {
            tx.send(pf(
                StreamPriority::Normal,
                Frame::Heartbeat { timestamp: i },
            ))
            .await
            .unwrap();
        }
        drop(tx);

        // Just verify connection doesn't drop immediately and we get data
        let mut buf = [0u8; 1024];
        let n = reader.read(&mut buf).await.unwrap();
        assert!(n > 0);
    }

    #[tokio::test]
    async fn test_batched_sender_data_frames() {
        let (tx, rx) = bounded_async::<PrioritizedFrame>(10);
        let (writer, mut reader) = duplex(65536);

        tokio::spawn(async move {
            run_batched_sender(rx, writer, TunnelCodec::new()).await;
        });

        for i in 0..3 {
            tx.send(pf(
                StreamPriority::Normal,
                Frame::Data {
                    stream_id: i,
                    data: Bytes::from("test data"),
                    end_of_stream: false,
                },
            ))
            .await
            .unwrap();
        }
        drop(tx);

        let mut buf = [0u8; 1024];
        let n = reader.read(&mut buf).await.unwrap();
        assert!(n > 0);
    }

    #[tokio::test]
    async fn test_immediate_flush_single_frame() {
        // Test that single frames are flushed immediately (no timeout delay)
        let (tx, rx) = bounded_async::<PrioritizedFrame>(10);
        let (writer, mut reader) = duplex(8192);

        tokio::spawn(async move {
            run_batched_sender(rx, writer, TunnelCodec::new()).await;
        });

        let start = Instant::now();
        tx.send(pf(
            StreamPriority::Normal,
            Frame::Heartbeat { timestamp: 1 },
        ))
        .await
        .unwrap();

        // Should receive quickly (not waiting for batch timeout)
        let mut buf = [0u8; 100];
        timeout(Duration::from_millis(10), reader.read(&mut buf))
            .await
            .expect("Should receive within 10ms (immediate flush)")
            .expect("Read should succeed");

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(20),
            "Single frame should flush immediately, took {:?}",
            elapsed
        );

        drop(tx);
    }

    #[tokio::test]
    async fn batched_sender_delivers_every_frame() {
        use futures::StreamExt;
        use tokio_util::codec::FramedRead;

        const N: u64 = 20_000;
        let (tx, rx) = bounded_async::<PrioritizedFrame>(64);
        let (client, server) = duplex(1 << 20);

        tokio::spawn(run_batched_sender(rx, server, TunnelCodec::new()));
        tokio::spawn(async move {
            for i in 0..N {
                tx.send((StreamPriority::Normal, Frame::Heartbeat { timestamp: i }))
                    .await
                    .unwrap();
            }
        });

        let mut framed = FramedRead::new(client, TunnelCodec::new());
        let mut seen = 0u64;
        while seen < N {
            match timeout(Duration::from_secs(5), framed.next()).await {
                Ok(Some(Ok(_))) => seen += 1,
                other => panic!("lost {} of {N} frames ({other:?})", N - seen),
            }
        }
    }
}
