use crate::traits::{Plugin, PluginAction, RequestContext};
use async_trait::async_trait;
use subtle::{Choice, ConstantTimeEq};

/// Token-based authentication plugin
pub struct TokenAuthPlugin {
    valid_tokens: Vec<String>,
    header_name: String,
}

fn constant_time_token_eq(expected: &[u8], presented: &[u8]) -> Choice {
    let max_len = expected.len().max(presented.len());
    let mut matches = expected.len().ct_eq(&presented.len());

    for i in 0..max_len {
        let expected_byte = expected.get(i).copied().unwrap_or(0);
        let presented_byte = presented.get(i).copied().unwrap_or(0);
        matches &= expected_byte.ct_eq(&presented_byte);
    }

    matches
}

impl TokenAuthPlugin {
    pub fn new(tokens: Vec<String>) -> Self {
        Self {
            valid_tokens: tokens,
            header_name: "X-Tunnel-Token".to_string(),
        }
    }

    #[must_use]
    pub fn with_header_name(mut self, name: String) -> Self {
        self.header_name = name;
        self
    }

    fn token_matches(&self, presented: &str) -> bool {
        let presented = presented.as_bytes();
        self.valid_tokens
            .iter()
            .fold(Choice::from(0), |matches, valid_token| {
                matches | constant_time_token_eq(valid_token.as_bytes(), presented)
            })
            .into()
    }
}

#[async_trait]
impl Plugin for TokenAuthPlugin {
    fn name(&self) -> &str {
        "token-auth"
    }

    async fn on_request(
        &self,
        req: &mut http::Request<()>,
        _ctx: &RequestContext,
    ) -> Result<PluginAction, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let token = req
            .headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok());

        match token {
            Some(t) if self.token_matches(t) => Ok(PluginAction::Continue),
            _ => Ok(PluginAction::Reject {
                status: 401,
                reason: format!("Invalid or missing {}", self.header_name),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_valid_token_allows_request() {
        let plugin = TokenAuthPlugin::new(vec!["secret123".to_string()]);

        let mut req = http::Request::builder()
            .header("X-Tunnel-Token", "secret123")
            .uri("/")
            .body(())
            .unwrap();

        let ctx = RequestContext {
            tunnel_id: "test".into(),
            session_id: "session".into(),
            remote_addr: "127.0.0.1:1234".parse().unwrap(),
            timestamp: std::time::SystemTime::now(),
        };

        let action = plugin.on_request(&mut req, &ctx).await.unwrap();
        assert_eq!(action, PluginAction::Continue);
    }

    #[tokio::test]
    async fn test_valid_token_allows_request_after_near_misses() {
        let plugin = TokenAuthPlugin::new(vec![
            "secret124".to_string(),
            "secret1234".to_string(),
            "secret123".to_string(),
        ]);

        let mut req = http::Request::builder()
            .header("X-Tunnel-Token", "secret123")
            .uri("/")
            .body(())
            .unwrap();

        let ctx = RequestContext {
            tunnel_id: "test".into(),
            session_id: "session".into(),
            remote_addr: "127.0.0.1:1234".parse().unwrap(),
            timestamp: std::time::SystemTime::now(),
        };

        let action = plugin.on_request(&mut req, &ctx).await.unwrap();
        assert_eq!(action, PluginAction::Continue);
    }

    #[tokio::test]
    async fn test_invalid_token_rejects_request() {
        let plugin = TokenAuthPlugin::new(vec!["secret123".to_string()]);

        let mut req = http::Request::builder()
            .header("X-Tunnel-Token", "wrong")
            .uri("/")
            .body(())
            .unwrap();

        let ctx = RequestContext {
            tunnel_id: "test".into(),
            session_id: "session".into(),
            remote_addr: "127.0.0.1:1234".parse().unwrap(),
            timestamp: std::time::SystemTime::now(),
        };

        let action = plugin.on_request(&mut req, &ctx).await.unwrap();

        match action {
            PluginAction::Reject { status, .. } => assert_eq!(status, 401),
            _ => panic!("Expected Reject"),
        }
    }

    #[tokio::test]
    async fn test_missing_token_rejects_request() {
        let plugin = TokenAuthPlugin::new(vec!["secret123".to_string()]);

        let mut req = http::Request::builder().uri("/").body(()).unwrap();

        let ctx = RequestContext {
            tunnel_id: "test".into(),
            session_id: "session".into(),
            remote_addr: "127.0.0.1:1234".parse().unwrap(),
            timestamp: std::time::SystemTime::now(),
        };

        let action = plugin.on_request(&mut req, &ctx).await.unwrap();

        match action {
            PluginAction::Reject { status, .. } => assert_eq!(status, 401),
            _ => panic!("Expected Reject"),
        }
    }

    #[test]
    fn constant_time_token_eq_requires_full_byte_and_length_match() {
        assert!(bool::from(constant_time_token_eq(
            b"secret123",
            b"secret123"
        )));
        assert!(!bool::from(constant_time_token_eq(
            b"secret123",
            b"secret124"
        )));
        assert!(!bool::from(constant_time_token_eq(
            b"secret123",
            b"secret1234"
        )));
        assert!(!bool::from(constant_time_token_eq(
            b"secret1234",
            b"secret123"
        )));
    }
}
