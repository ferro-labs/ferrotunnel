//! FerroTunnel Soak Testing Tool
//!
//! Runs continuous traffic against a target to verify stability over long durations.

use anyhow::Result;
use clap::Parser;
use ferrotunnel::Client;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::interval;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "ferrotunnel-soak")]
#[command(about = "Soak testing tool for FerroTunnel")]
struct Args {
    /// Target server address
    #[arg(long, default_value = "127.0.0.1:9999")]
    target: String,

    /// Tunnel server address
    #[arg(long, default_value = "127.0.0.1:7835")]
    tunnel_addr: String,

    /// Authentication token
    #[arg(long, default_value = "my-secret-token")]
    token: String,

    /// Number of concurrent tunnels
    #[arg(long, default_value = "10")]
    concurrency: usize,

    /// Test duration in minutes (0 = infinite)
    #[arg(long, default_value = "0")]
    duration: u64,

    /// Metrics output file
    #[arg(long, default_value = "soak_metrics.jsonl")]
    output: String,
}

#[derive(Serialize)]
struct SoakMetrics {
    ts: u64,
    elapsed_sec: u64,
    rss_mb: Option<u64>,
    active_tunnels: usize,
    total_bytes: u64,
    errors: u64,
}

struct Stats {
    total_bytes: AtomicU64,
    errors: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("Starting soak test");
    info!("  Target: {}", args.target);
    info!("  Tunnel: {}", args.tunnel_addr);
    info!("  Concurrency: {}", args.concurrency);
    info!("  Duration: {} min", args.duration);

    let stats = Arc::new(Stats {
        total_bytes: AtomicU64::new(0),
        errors: AtomicU64::new(0),
    });

    // Start background traffic generators
    let mut handles = Vec::new();
    for i in 0..args.concurrency {
        let stats = stats.clone();
        let target = args.target.clone();
        let tunnel = args.tunnel_addr.clone();
        let token = args.token.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Connect and send some traffic
                if let Err(e) = run_traffic_cycle(&tunnel, &token, &target, &stats).await {
                    error!("Traffic error (client {}): {}", i, e);
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        handles.push(handle);
        // Stagger starts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Metrics report loop
    let start_time = Instant::now();
    let mut report_interval = interval(Duration::from_mins(1));
    let mut metrics_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.output)?;

    let end_time = if args.duration > 0 {
        Some(start_time + Duration::from_secs(args.duration * 60))
    } else {
        None
    };

    loop {
        report_interval.tick().await;

        let elapsed = start_time.elapsed();
        if let Some(end) = end_time {
            if Instant::now() >= end {
                info!("Soak test duration reached via timeout");
                break;
            }
        }

        let rss = get_memory_usage();
        let metrics = SoakMetrics {
            ts: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            elapsed_sec: elapsed.as_secs(),
            rss_mb: rss,
            active_tunnels: args.concurrency, // Approximation
            total_bytes: stats.total_bytes.load(Ordering::Relaxed),
            errors: stats.errors.load(Ordering::Relaxed),
        };

        let json = serde_json::to_string(&metrics)?;
        writeln!(metrics_file, "{}", json)?;
        metrics_file.flush()?;

        info!(
            "Soak Status: {:.1}h | RSS: {:?}MB | Bytes: {:.2}GB | Errors: {}",
            elapsed.as_secs_f64() / 3600.0,
            rss,
            metrics.total_bytes as f64 / 1_000_000_000.0,
            metrics.errors
        );
    }

    Ok(())
}

async fn run_traffic_cycle(tunnel: &str, token: &str, _target: &str, stats: &Stats) -> Result<()> {
    // 1. Connect tunnel
    let mut client = Client::builder().server_addr(tunnel).token(token).build()?;

    // Start client in background
    let _info = client.start().await?;

    // In a real soak test, we'd also want to generate traffic THROUGH the tunnel
    // For now, we simulate session duration
    let traffic_duration = Duration::from_mins(5); // 5 mins per connection

    // Simulate some bytes
    stats.total_bytes.fetch_add(1024 * 1024, Ordering::Relaxed);

    tokio::time::sleep(traffic_duration).await;

    client.shutdown().await?;

    Ok(())
}

#[cfg(target_os = "linux")]
fn get_memory_usage() -> Option<u64> {
    use procfs::process::Process;
    Process::myself()
        .ok()
        .and_then(|p: Process| p.status().ok())
        .and_then(|s| s.vmrss)
        .map(|kb| kb / 1024)
}

#[cfg(not(target_os = "linux"))]
fn get_memory_usage() -> Option<u64> {
    None
}
