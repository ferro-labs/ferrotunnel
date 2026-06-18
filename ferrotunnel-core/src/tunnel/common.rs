use ferrotunnel_common::{Result, TunnelError};
use ferrotunnel_protocol::frame::Frame;
use futures::{Stream, StreamExt};
use std::time::Duration;
use tokio::time::timeout;

pub fn clamp_u128_to_u64(i: u128) -> u64 {
    i.min(u128::from(u64::MAX)) as u64
}

/// Read the first frame of a handshake, enforcing a timeout.
///
/// Shared by the client and server handshake paths. `context` is used only to
/// disambiguate the error messages (e.g. `"client"` / `"server"`).
pub(crate) async fn read_initial_handshake_frame<S>(
    framed: &mut S,
    handshake_timeout: Duration,
    context: &str,
) -> Result<Frame>
where
    S: Stream<Item = std::io::Result<Frame>> + Unpin,
{
    let result = timeout(handshake_timeout, framed.next())
        .await
        .map_err(|_| {
            TunnelError::Timeout(format!(
                "{context} handshake timed out after {handshake_timeout:?}"
            ))
        })?;

    result
        .ok_or_else(|| TunnelError::Connection(format!("{context} handshake stream closed")))?
        .map_err(TunnelError::Io)
}
