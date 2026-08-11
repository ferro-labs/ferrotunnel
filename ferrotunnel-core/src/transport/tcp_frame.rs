//! TCP implementation of [`FrameSender`] and [`FrameReceiver`].

use super::{FrameReceiver, FrameSender};
use crate::stream::PrioritizedFrame;
use ferrotunnel_common::Result;
use ferrotunnel_protocol::codec::TunnelCodec;
use ferrotunnel_protocol::frame::StreamPriority;
use ferrotunnel_protocol::Frame;
use futures::StreamExt;
use kanal::AsyncSender;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio_util::codec::FramedRead;

/// Maximum time to keep retrying a push to the batched-sender channel before
/// treating the peer as unreachable. Sends retry `try_send_option` with a
/// short backoff and fail past this deadline, so a full or closed channel
/// during teardown cannot stall the caller (#136).
const FRAME_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Sends frames over TCP by pushing to the channel consumed by the batched sender task.
#[derive(Clone)]
pub struct TcpFrameSender {
    tx: AsyncSender<PrioritizedFrame>,
}

impl TcpFrameSender {
    pub fn new(tx: AsyncSender<PrioritizedFrame>) -> Self {
        Self { tx }
    }
}

impl FrameSender for TcpFrameSender {
    fn send_frame(&self, frame: Frame) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            // Trait API has no priority; use Normal when sending via trait.
            // Fail fast on a full/closed channel instead of blocking forever (#136).
            let mut item = Some((StreamPriority::Normal, frame));
            let deadline = tokio::time::Instant::now() + FRAME_SEND_TIMEOUT;
            let mut backoff = Duration::from_millis(1);
            loop {
                match tx.try_send_option(&mut item) {
                    Ok(true) => return Ok(()),
                    Ok(false) => {
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            return Err(ferrotunnel_common::TunnelError::Protocol(
                                "timed out sending frame to wire channel".into(),
                            ));
                        }
                        tokio::time::sleep(backoff.min(deadline.duration_since(now))).await;
                        backoff = (backoff * 2).min(Duration::from_millis(10));
                    }
                    Err(e) => return Err(ferrotunnel_common::TunnelError::Protocol(e.to_string())),
                }
            }
        })
    }
}

/// Receives frames from TCP via a framed read stream.
pub struct TcpFrameReceiver<R> {
    stream: FramedRead<R, TunnelCodec>,
}

impl<R> TcpFrameReceiver<R>
where
    R: AsyncRead + Unpin + Send,
{
    pub fn new(stream: FramedRead<R, TunnelCodec>) -> Self {
        Self { stream }
    }
}

impl<R> FrameReceiver for TcpFrameReceiver<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn recv_frame(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + '_>> {
        let stream = &mut self.stream;
        Box::pin(async move {
            match stream.next().await {
                Some(Ok(frame)) => Ok(Some(frame)),
                Some(Err(e)) => Err(ferrotunnel_common::TunnelError::Io(e)),
                None => Ok(None),
            }
        })
    }
}
