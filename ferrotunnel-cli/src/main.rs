//! FerroTunnel Unified CLI
//!
//! A secure, high-performance reverse tunnel system.

// Use mimalloc as the global allocator for better performance
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod commands;
mod middleware;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "ferrotunnel",
    author,
    version,
    about = "Secure, high-performance reverse tunnel system",
    long_about = "FerroTunnel is a secure, high-performance reverse tunnel system in Rust.\n\n\
                  It can be used as a CLI tool or embedded directly into your applications.",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the tunnel server
    Server(commands::server::ServerArgs),

    /// Run the tunnel client
    Client(commands::client::ClientArgs),

    /// Show version information
    Version,
}

#[tokio::main]
async fn main() -> Result<()> {
    // The rustls crypto provider is installed on demand by the library's TLS
    // setup (`ferrotunnel-core` transport and the HTTP/3 ingress) before any TLS
    // config is built, so the CLI does not install it here.
    let cli = Cli::parse();

    match cli.command {
        Commands::Server(args) => commands::server::run(args).await,
        Commands::Client(args) => commands::client::run(args).await,
        Commands::Version => {
            commands::version::run();
            Ok(())
        }
    }
}
