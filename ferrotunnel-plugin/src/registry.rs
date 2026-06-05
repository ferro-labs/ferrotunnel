use crate::traits::{Plugin, PluginAction, RequestContext, ResponseContext};
use std::error::Error;
use std::io;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinError;

type PluginError = Box<dyn Error + Send + Sync + 'static>;
type PluginResult<T> = Result<T, PluginError>;

/// Registry manages all loaded plugins
#[derive(Default)]
pub struct PluginRegistry {
    plugins: Vec<Arc<RwLock<dyn Plugin>>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    /// Register a plugin
    pub fn register(&mut self, plugin: Arc<RwLock<dyn Plugin>>) {
        self.plugins.push(plugin);
    }

    /// Initialize all plugins
    pub async fn init_all(&self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        for plugin_lock in &self.plugins {
            let mut plugin = plugin_lock.write().await;
            tracing::info!("Initializing plugin: {}", plugin.name());
            plugin.init().await?;
        }
        Ok(())
    }

    /// Execute request hooks on all plugins
    pub async fn execute_request_hooks(
        &self,
        req: &mut http::Request<()>,
        ctx: &RequestContext,
    ) -> Result<PluginAction, Box<dyn Error + Send + Sync + 'static>> {
        let mut current_req = std::mem::replace(req, http::Request::new(()));
        for plugin_lock in &self.plugins {
            let plugin_name = plugin_name(plugin_lock).await;
            let plugin_lock = plugin_lock.clone();
            let ctx = ctx.clone();
            let hook = tokio::spawn(run_request_hook(plugin_lock, current_req, ctx));
            let (next_req, action) = match hook.await {
                Ok(result) => result,
                Err(err) => return Err(join_error(&plugin_name, "on_request", &err)),
            };
            current_req = next_req;

            match action? {
                PluginAction::Continue => continue,
                action => {
                    *req = current_req;
                    return Ok(action); // Short-circuit on non-Continue
                }
            }
        }
        *req = current_req;
        Ok(PluginAction::Continue)
    }

    /// Execute response hooks on all plugins
    pub async fn execute_response_hooks(
        &self,
        res: &mut http::Response<Vec<u8>>,
        ctx: &ResponseContext,
    ) -> Result<PluginAction, Box<dyn Error + Send + Sync + 'static>> {
        let mut current_res = std::mem::replace(res, http::Response::new(Vec::new()));
        for plugin_lock in &self.plugins {
            let plugin_name = plugin_name(plugin_lock).await;
            let plugin_lock = plugin_lock.clone();
            let ctx = ctx.clone();
            let hook = tokio::spawn(run_response_hook(plugin_lock, current_res, ctx));
            let (next_res, action) = match hook.await {
                Ok(result) => result,
                Err(err) => return Err(join_error(&plugin_name, "on_response", &err)),
            };
            current_res = next_res;

            match action? {
                PluginAction::Continue => continue,
                action => {
                    *res = current_res;
                    return Ok(action);
                }
            }
        }
        *res = current_res;
        Ok(PluginAction::Continue)
    }

    /// Shutdown all plugins
    pub async fn shutdown_all(&self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        for plugin_lock in &self.plugins {
            let mut plugin = plugin_lock.write().await;
            tracing::info!("Shutting down plugin: {}", plugin.name());
            plugin.shutdown().await?;
        }
        Ok(())
    }

    /// Returns true if any plugin needs to inspect/modify response bodies.
    /// When false, responses can be streamed without buffering for better performance.
    pub async fn needs_response_buffering(&self) -> bool {
        for plugin_lock in &self.plugins {
            let plugin = plugin_lock.read().await;
            if plugin.needs_response_body() {
                return true;
            }
        }
        false
    }

    /// Returns true if no plugins are registered
    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }
}

async fn plugin_name(plugin_lock: &Arc<RwLock<dyn Plugin>>) -> String {
    let plugin = plugin_lock.read().await;
    plugin.name().to_owned()
}

async fn run_request_hook(
    plugin_lock: Arc<RwLock<dyn Plugin>>,
    mut req: http::Request<()>,
    ctx: RequestContext,
) -> (http::Request<()>, PluginResult<PluginAction>) {
    let plugin = plugin_lock.read().await;
    let action = plugin.on_request(&mut req, &ctx).await;
    (req, action)
}

async fn run_response_hook(
    plugin_lock: Arc<RwLock<dyn Plugin>>,
    mut res: http::Response<Vec<u8>>,
    ctx: ResponseContext,
) -> (http::Response<Vec<u8>>, PluginResult<PluginAction>) {
    let plugin = plugin_lock.read().await;
    let action = plugin.on_response(&mut res, &ctx).await;
    (res, action)
}

fn join_error(plugin_name: &str, hook: &str, err: &JoinError) -> PluginError {
    if err.is_panic() {
        tracing::error!(plugin = plugin_name, hook, "plugin hook panicked");
        Box::new(io::Error::other(format!(
            "plugin `{plugin_name}` panicked during {hook} hook"
        )))
    } else {
        tracing::error!(plugin = plugin_name, hook, error = %err, "plugin hook task failed");
        Box::new(io::Error::other(format!(
            "plugin `{plugin_name}` {hook} hook task failed: {err}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    // Test plugin that always rejects
    struct RejectPlugin;
    #[async_trait]
    impl Plugin for RejectPlugin {
        fn name(&self) -> &str {
            "rejector"
        }

        async fn on_request(
            &self,
            _req: &mut http::Request<()>,
            _ctx: &RequestContext,
        ) -> Result<PluginAction, Box<dyn Error + Send + Sync + 'static>> {
            Ok(PluginAction::Reject {
                status: 403,
                reason: "Blocked".into(),
            })
        }
    }

    // Test plugin that always continues
    struct PassthroughPlugin;
    #[async_trait]
    impl Plugin for PassthroughPlugin {
        fn name(&self) -> &str {
            "passthrough"
        }
    }

    // Test plugin that needs response body
    struct BodyInspectorPlugin;
    #[async_trait]
    impl Plugin for BodyInspectorPlugin {
        fn name(&self) -> &str {
            "body-inspector"
        }

        fn needs_response_body(&self) -> bool {
            true
        }
    }

    struct PanicRequestPlugin;
    #[async_trait]
    impl Plugin for PanicRequestPlugin {
        fn name(&self) -> &str {
            "panic-request"
        }

        async fn on_request(
            &self,
            _req: &mut http::Request<()>,
            _ctx: &RequestContext,
        ) -> Result<PluginAction, Box<dyn Error + Send + Sync + 'static>> {
            panic!("request hook panic")
        }
    }

    struct PanicResponsePlugin;
    #[async_trait]
    impl Plugin for PanicResponsePlugin {
        fn name(&self) -> &str {
            "panic-response"
        }

        async fn on_response(
            &self,
            _res: &mut http::Response<Vec<u8>>,
            _ctx: &ResponseContext,
        ) -> Result<PluginAction, Box<dyn Error + Send + Sync + 'static>> {
            panic!("response hook panic")
        }
    }

    fn make_request_ctx() -> RequestContext {
        RequestContext {
            tunnel_id: "test".into(),
            session_id: "sess".into(),
            remote_addr: "127.0.0.1:80".parse().unwrap(),
            timestamp: std::time::SystemTime::now(),
        }
    }

    fn make_response_ctx() -> ResponseContext {
        ResponseContext {
            tunnel_id: "test".into(),
            session_id: "sess".into(),
            status_code: 200,
            duration_ms: 1,
        }
    }

    #[test]
    fn test_registry_new_is_empty() {
        let registry = PluginRegistry::new();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_registry_not_empty_after_register() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(PassthroughPlugin)));
        assert!(!registry.is_empty());
    }

    #[tokio::test]
    async fn test_registry_executes_plugins() {
        let plugin = Arc::new(RwLock::new(RejectPlugin));
        let mut registry = PluginRegistry::new();
        registry.register(plugin);

        let mut req = http::Request::builder().body(()).unwrap();
        let ctx = make_request_ctx();

        let action = registry
            .execute_request_hooks(&mut req, &ctx)
            .await
            .unwrap();
        match action {
            PluginAction::Reject { status, .. } => assert_eq!(status, 403),
            _ => panic!("Expected reject"),
        }
    }

    #[tokio::test]
    async fn test_registry_empty_returns_continue() {
        let registry = PluginRegistry::new();
        let mut req = http::Request::builder().body(()).unwrap();
        let ctx = make_request_ctx();

        let action = registry
            .execute_request_hooks(&mut req, &ctx)
            .await
            .unwrap();
        assert_eq!(action, PluginAction::Continue);
    }

    #[tokio::test]
    async fn test_registry_passthrough_returns_continue() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(PassthroughPlugin)));

        let mut req = http::Request::builder().body(()).unwrap();
        let ctx = make_request_ctx();

        let action = registry
            .execute_request_hooks(&mut req, &ctx)
            .await
            .unwrap();
        assert_eq!(action, PluginAction::Continue);
    }

    #[tokio::test]
    async fn test_registry_init_all_success() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(PassthroughPlugin)));

        let result = registry.init_all().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_registry_shutdown_all_success() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(PassthroughPlugin)));

        let result = registry.shutdown_all().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_registry_needs_response_buffering_false_when_empty() {
        let registry = PluginRegistry::new();
        assert!(!registry.needs_response_buffering().await);
    }

    #[tokio::test]
    async fn test_registry_needs_response_buffering_detects_plugin() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(BodyInspectorPlugin)));
        assert!(registry.needs_response_buffering().await);
    }

    #[tokio::test]
    async fn test_registry_short_circuits_on_reject() {
        let mut registry = PluginRegistry::new();
        // RejectPlugin first, then PassthroughPlugin
        registry.register(Arc::new(RwLock::new(RejectPlugin)));
        registry.register(Arc::new(RwLock::new(PassthroughPlugin)));

        let mut req = http::Request::builder().body(()).unwrap();
        let ctx = make_request_ctx();

        let action = registry
            .execute_request_hooks(&mut req, &ctx)
            .await
            .unwrap();
        // Should get Reject (short-circuited), not Continue
        match action {
            PluginAction::Reject { status, .. } => assert_eq!(status, 403),
            _ => panic!("Expected reject - should short-circuit"),
        }
    }

    #[tokio::test]
    async fn test_request_hook_panic_is_reported_as_error() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(PanicRequestPlugin)));

        let join = tokio::spawn(async move {
            let mut req = http::Request::new(());
            let ctx = make_request_ctx();
            registry.execute_request_hooks(&mut req, &ctx).await
        });

        let result = match join.await {
            Ok(result) => result,
            Err(err) => panic!("registry task should not panic: {err}"),
        };
        let err = match result {
            Ok(action) => panic!("expected plugin error, got {action:?}"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("panic-request"));
        assert!(msg.contains("on_request"));
    }

    #[tokio::test]
    async fn test_response_hook_panic_is_reported_as_error() {
        let mut registry = PluginRegistry::new();
        registry.register(Arc::new(RwLock::new(PanicResponsePlugin)));

        let join = tokio::spawn(async move {
            let mut res = http::Response::new(Vec::new());
            let ctx = make_response_ctx();
            registry.execute_response_hooks(&mut res, &ctx).await
        });

        let result = match join.await {
            Ok(result) => result,
            Err(err) => panic!("registry task should not panic: {err}"),
        };
        let err = match result {
            Ok(action) => panic!("expected plugin error, got {action:?}"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("panic-response"));
        assert!(msg.contains("on_response"));
    }
}
