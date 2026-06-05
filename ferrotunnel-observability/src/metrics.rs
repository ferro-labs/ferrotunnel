//! Prometheus metrics for FerroTunnel.
//!
//! Naming follows [Prometheus best practices](https://prometheus.io/docs/practices/naming/):
//! - **Counters**: suffix `_total`
//! - **Units**: in the name (e.g. `_seconds`, `_bytes`)
//! - **Gauges**: descriptive names, no `_total`

use anyhow::Context;
use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};
use std::sync::LazyLock;
use std::sync::Once;
use std::sync::OnceLock;
use std::time::Duration;

use prometheus::Registry;

/// Default registry (for custom collectors or dashboard). Tunnel metrics use the global default registry.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// Global tunnel metrics. Set when [`init_metrics`] is called.
static TUNNEL_METRICS: OnceLock<TunnelMetrics> = OnceLock::new();
static METRICS_INIT: Once = Once::new();

/// Tunnel-level metrics: frames, bytes, decode/encode latency, queue depth.
///
/// All values are exported to Prometheus when [`gather_metrics`] is called.
#[derive(Debug)]
pub struct TunnelMetrics {
    frames_processed: Counter,
    bytes_transferred: Counter,
    decode_latency: Histogram,
    encode_latency: Histogram,
    queue_depth: Gauge,
}

impl TunnelMetrics {
    /// Create and register metrics with the default Prometheus registry.
    pub fn new() -> Self {
        Self::try_new().expect("register FerroTunnel tunnel metrics")
    }

    /// Try to create and register metrics with the default Prometheus registry.
    pub fn try_new() -> Result<Self, prometheus::Error> {
        let frames_processed = register_counter!(
            "ferrotunnel_tunnel_frames_processed_total",
            "Total number of protocol frames processed (decoded or encoded)"
        )?;

        let bytes_transferred = register_counter!(
            "ferrotunnel_tunnel_bytes_transferred_total",
            "Total bytes transferred through the tunnel (data frames only)"
        )?;

        let decode_latency = register_histogram!(
            "ferrotunnel_tunnel_decode_latency_seconds",
            "Latency of decoding frames from the wire"
        )?;

        let encode_latency = register_histogram!(
            "ferrotunnel_tunnel_encode_latency_seconds",
            "Latency of encoding frames to the wire"
        )?;

        let queue_depth = register_gauge!(
            "ferrotunnel_tunnel_queue_depth",
            "Current number of frames queued in the batched sender"
        )?;

        Ok(Self {
            frames_processed,
            bytes_transferred,
            decode_latency,
            encode_latency,
            queue_depth,
        })
    }

    /// Record a decode operation (frames decoded, bytes, and latency).
    #[inline]
    pub fn record_decode(&self, frames: usize, bytes: usize, latency: Duration) {
        self.frames_processed.inc_by(frames as f64);
        self.bytes_transferred.inc_by(bytes as f64);
        self.decode_latency.observe(latency.as_secs_f64());
    }

    /// Record an encode operation (frames encoded, bytes, and latency).
    #[inline]
    pub fn record_encode(&self, frames: usize, bytes: usize, latency: Duration) {
        self.frames_processed.inc_by(frames as f64);
        self.bytes_transferred.inc_by(bytes as f64);
        self.encode_latency.observe(latency.as_secs_f64());
    }

    /// Set the current sender queue depth (gauge).
    #[inline]
    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.set(depth as f64);
    }

    /// Record bytes transferred (e.g. from TCP ingress bidirectional copy).
    #[inline]
    pub fn record_bytes(&self, bytes: usize) {
        self.bytes_transferred.inc_by(bytes as f64);
    }
}

impl Default for TunnelMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the global tunnel metrics, if metrics have been initialized.
#[inline]
pub fn tunnel_metrics() -> Option<&'static TunnelMetrics> {
    TUNNEL_METRICS.get()
}

/// Initialize the metrics system and register tunnel metrics.
pub fn init_metrics() {
    let _ = LazyLock::force(&REGISTRY);
    if TUNNEL_METRICS.get().is_some() {
        return;
    }

    METRICS_INIT.call_once(|| match TunnelMetrics::try_new() {
        Ok(metrics) => {
            let _ = TUNNEL_METRICS.set(metrics);
            tracing::info!("Metrics infrastructure initialized");
        }
        Err(error) => {
            tracing::error!(%error, "failed to initialize metrics infrastructure");
        }
    });
}

/// Returns true if metrics have been initialized.
#[inline]
pub fn metrics_enabled() -> bool {
    TUNNEL_METRICS.get().is_some()
}

/// Gather all metrics into Prometheus text format.
pub fn gather_metrics() -> anyhow::Result<String> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .context("failed to encode Prometheus metrics")?;
    String::from_utf8(buffer).context("Prometheus metrics output was not valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::{gather_metrics, init_metrics, metrics_enabled};

    #[test]
    fn init_metrics_is_idempotent_and_scrape_does_not_panic() {
        init_metrics();
        init_metrics();

        assert!(metrics_enabled());
        let metrics = gather_metrics().expect("metrics scrape should succeed");

        assert!(metrics.contains("ferrotunnel_tunnel_frames_processed_total"));
    }
}
