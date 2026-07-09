# FerroTunnel Plugin System

[![Crates.io](https://img.shields.io/crates/v/ferrotunnel-plugin.svg)](https://crates.io/crates/ferrotunnel-plugin)
[![Documentation](https://docs.rs/ferrotunnel-plugin/badge.svg)](https://docs.rs/ferrotunnel-plugin)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](../LICENSE-MIT)

This crate provides plugin traits, built-in plugins, and the `PluginRegistry` used by FerroTunnel HTTP ingress.

## Create a plugin

Implement `Plugin` and override only the hooks the plugin needs:

```rust
use async_trait::async_trait;
use ferrotunnel_plugin::{Plugin, PluginAction, RequestContext};

struct BlockAdmin;

#[async_trait]
impl Plugin for BlockAdmin {
    fn name(&self) -> &str {
        "block-admin"
    }

    async fn on_request(
        &self,
        request: &mut http::Request<()>,
        _context: &RequestContext,
    ) -> Result<PluginAction, Box<dyn std::error::Error + Send + Sync>> {
        if request.uri().path() == "/admin" {
            return Ok(PluginAction::Reject {
                status: 403,
                reason: "Access denied".to_string(),
            });
        }

        Ok(PluginAction::Continue)
    }
}
```

## Register and run hooks

`PluginRegistry` exposes explicit lifecycle and hook methods:

```rust,no_run
use std::sync::Arc;
use ferrotunnel_plugin::PluginRegistry;
use tokio::sync::RwLock;

# struct BlockAdmin;
# #[async_trait::async_trait]
# impl ferrotunnel_plugin::Plugin for BlockAdmin {
#     fn name(&self) -> &str { "block-admin" }
# }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut registry = PluginRegistry::new();
registry.register(Arc::new(RwLock::new(BlockAdmin)));
registry.init_all().await?;

// Pass the registry to a lower-level ingress integration, then shut it down
// when the owning service stops.
registry.shutdown_all().await?;
# Ok(())
# }
```

The `on_request` and `on_response` hooks are used by HTTP ingress when it is constructed with this registry. The high-level `ServerBuilder` does not currently expose custom plugin registration, and the running server does not invoke `on_stream_data` automatically. Applications using the registry directly must call `init_all` and `shutdown_all`.

## Actions

- `PluginAction::Continue` passes control to the next plugin.
- `PluginAction::Reject` stops the chain and returns an error response.
- `PluginAction::Respond` stops the chain and returns a custom response.
- `PluginAction::Modify` records that the request or response was modified and continues processing.

## Built-in plugins

See [`src/builtin/`](src/builtin/) for the logger, token-authentication, rate-limit, and circuit-breaker plugins. The crate unit tests demonstrate registry execution, short-circuiting, response buffering, and panic isolation.

## Testing guidance

- Exercise each returned action, including failure paths.
- Keep hooks non-blocking; move blocking work to `spawn_blocking`.
- Return errors instead of panicking. The registry isolates request and response hook panics, but normal error handling preserves better diagnostics.
- Override `needs_response_body` only when response buffering is required.

API details are available on [docs.rs](https://docs.rs/ferrotunnel-plugin).
