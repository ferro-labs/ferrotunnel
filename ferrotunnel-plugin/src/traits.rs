use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Action that a plugin can return
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PluginAction {
    /// Continue processing (allow other plugins to run)
    Continue,

    /// Reject the request with status and reason
    Reject { status: u16, reason: String },

    /// Reserved for in-place modification. It carries no modification data yet,
    /// and the registry stops the hook chain on it as with any non-`Continue` action.
    Modify {
        // Placeholder for modification logic
    },

    /// Short-circuit and respond immediately
    Respond {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
}

/// Request context passed to plugins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestContext {
    pub tunnel_id: String,
    pub session_id: String,
    pub remote_addr: std::net::SocketAddr,
    pub timestamp: std::time::SystemTime,
}

/// Response context passed to plugins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseContext {
    pub tunnel_id: String,
    pub session_id: String,
    pub status_code: u16,
    pub duration_ms: u64,
}

/// Stream context for data plugins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamContext {
    pub tunnel_id: String,
    pub stream_id: u32,
    pub direction: StreamDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StreamDirection {
    Inbound,  // Internet -> Local
    Outbound, // Local -> Internet
}

/// Core plugin trait
///
/// Hook implementations should return `Err` for recoverable failures and avoid
/// panicking. `PluginRegistry` isolates `on_request` and `on_response` panics,
/// logs the offending plugin name, and converts the panic into a normal plugin
/// error so ingress handlers can return a clean 500 response instead of dropping
/// the connection task.
#[async_trait]
pub trait Plugin: Send + Sync {
    /// Plugin name (for logging/debugging)
    fn name(&self) -> &str;

    /// Plugin version
    fn version(&self) -> &str {
        "1.0.0"
    }

    /// Initialize plugin (called once on startup)
    async fn init(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    /// Shutdown plugin (called on graceful shutdown)
    async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }

    /// Hook: Before request is proxied
    async fn on_request(
        &self,
        _req: &mut http::Request<()>,
        _ctx: &RequestContext,
    ) -> Result<PluginAction, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(PluginAction::Continue)
    }

    /// Hook: After response received from local server
    async fn on_response(
        &self,
        _res: &mut http::Response<Vec<u8>>,
        _ctx: &ResponseContext,
    ) -> Result<PluginAction, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(PluginAction::Continue)
    }

    /// Returns true if this plugin needs to inspect/modify response bodies.
    /// Override to return true if your plugin uses on_response with body access.
    /// When false, responses can be streamed without buffering for better performance.
    fn needs_response_body(&self) -> bool {
        false
    }

    /// Hook: When stream data flows through tunnel
    async fn on_stream_data(
        &self,
        _data: &mut Vec<u8>,
        _ctx: &StreamContext,
    ) -> Result<PluginAction, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(PluginAction::Continue)
    }
}
