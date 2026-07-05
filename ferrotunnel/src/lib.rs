//! # `FerroTunnel`
//!
//! A production-ready, secure reverse tunnel system in Rust.
//!
//! `FerroTunnel` is the **first embeddable Rust reverse tunnel**, allowing you to
//! integrate tunneling directly into your applications with a simple builder API.
//!
//! ## Features
//!
//! - 🔒 **Secure** - Token-based authentication
//! - ⚡ **Fast** - Built on Tokio for high-performance async I/O
//! - 🔌 **Embeddable** - Use as a library in your own applications
//! - 🛡️ **Resilient** - Automatic reconnection, heartbeat monitoring
//!
//! ## Quick Start: Embedded Client
//!
//! ```rust,no_run
//! use ferrotunnel::Client;
//!
//! #[tokio::main]
//! async fn main() -> ferrotunnel::Result<()> {
//!     let mut client = Client::builder()
//!         .server_addr("tunnel.example.com:7835")
//!         .token("my-secret-token")
//!         .local_addr("127.0.0.1:8080")
//!         .build()?;
//!
//!     let info = client.start().await?;
//!     println!("Connected! Session: {:?}", info.session_id());
//!
//!     // Keep running until Ctrl+C
//!     tokio::signal::ctrl_c().await?;
//!     client.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Quick Start: Embedded Server
//!
//! ```rust,no_run
//! use ferrotunnel::Server;
//!
//! #[tokio::main]
//! async fn main() -> ferrotunnel::Result<()> {
//!     let mut server = Server::builder()
//!         .bind("0.0.0.0:7835".parse().unwrap())
//!         .http_bind("0.0.0.0:8080".parse().unwrap())
//!         .token("my-secret-token")
//!         .build()?;
//!
//!     server.start().await?;
//!     tokio::signal::ctrl_c().await?;
//!     server.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! `FerroTunnel` consists of several crates:
//!
//! - `ferrotunnel` - Main library with builder API (this crate)
//! - `ferrotunnel-common` - Shared types, errors, and utilities
//! - `ferrotunnel-protocol` - Wire protocol definitions and codec
//! - `ferrotunnel-core` - Core tunnel implementation
//! - `ferrotunnel-http` - HTTP ingress and proxy
//!
//! ## Re-exports
//!
//! This crate re-exports the most commonly used items from the subcrates
//! for convenience.

// Modules
pub mod client;
pub mod config;
pub mod server;

// Re-export the shared vocabulary crate (errors, `Result`, config types).
pub use ferrotunnel_common as common;

// Implementation crates are re-exported for advanced use but are not part of
// the stable public API surface, so they are hidden from the generated docs.
// The prelude re-exports the specific protocol types intended for public use.
#[doc(hidden)]
pub use ferrotunnel_core as core;
#[doc(hidden)]
pub use ferrotunnel_http as http;
#[doc(hidden)]
pub use ferrotunnel_protocol as protocol;

// Public API exports
pub use client::{Client, ClientBuilder};
pub use config::{ClientConfig, ServerConfig, TunnelInfo};
pub use server::{Server, ServerBuilder};

/// Prelude module for convenient imports
pub mod prelude {
    // Builder API
    pub use crate::client::{Client, ClientBuilder};
    pub use crate::config::{ClientConfig, ServerConfig, TunnelInfo};
    pub use crate::server::{Server, ServerBuilder};

    // Common types
    pub use crate::common::{Result, TunnelError};

    // Protocol types
    pub use crate::protocol::{
        CloseReason, Frame, HandshakeStatus, Protocol, RegisterStatus, StreamStatus, TunnelCodec,
    };
}

// Convenience re-exports at crate root
pub use common::{Result, TunnelError};
pub use protocol::{
    CloseReason, Frame, HandshakeStatus, Protocol, RegisterStatus, StreamStatus, TunnelCodec,
};
