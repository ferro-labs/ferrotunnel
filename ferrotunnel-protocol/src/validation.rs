//! Frame validation for security hardening

use crate::frame::Frame;
use ferrotunnel_common::LimitsConfig;

/// Validation errors
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Frame too large: {size} bytes exceeds limit of {limit} bytes")]
    FrameTooLarge { size: u64, limit: u64 },

    #[error("Token too long: {len} bytes exceeds limit of {limit} bytes")]
    TokenTooLong { len: usize, limit: usize },

    #[error("Too many capabilities: {count} exceeds limit of {limit}")]
    TooManyCapabilities { count: usize, limit: usize },

    #[error("Capability too long: {len} bytes exceeds limit of {limit} bytes")]
    CapabilityTooLong { len: usize, limit: usize },

    #[error("Payload too large: {size} bytes exceeds limit of {limit} bytes")]
    PayloadTooLarge { size: usize, limit: usize },
}

/// Validation limits
#[derive(Debug, Clone)]
pub struct ValidationLimits {
    pub max_frame_bytes: u64,
    pub max_token_len: usize,
    pub max_capabilities: usize,
    pub max_capability_len: usize,
    pub max_payload_bytes: usize,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self::from(&LimitsConfig::default())
    }
}

impl From<&LimitsConfig> for ValidationLimits {
    fn from(limits: &LimitsConfig) -> Self {
        Self {
            max_frame_bytes: limits.max_frame_bytes,
            max_token_len: limits.max_token_len,
            max_capabilities: limits.max_capabilities,
            max_capability_len: limits.max_capability_len,
            max_payload_bytes: usize::try_from(limits.max_frame_bytes).unwrap_or(usize::MAX),
        }
    }
}

/// Validate a decoded frame against limits
pub fn validate_frame(frame: &Frame, limits: &ValidationLimits) -> Result<(), ValidationError> {
    match frame {
        Frame::Handshake(handshake) => {
            if handshake.token.len() > limits.max_token_len {
                return Err(ValidationError::TokenTooLong {
                    len: handshake.token.len(),
                    limit: limits.max_token_len,
                });
            }
            if handshake.capabilities.len() > limits.max_capabilities {
                return Err(ValidationError::TooManyCapabilities {
                    count: handshake.capabilities.len(),
                    limit: limits.max_capabilities,
                });
            }
            for cap in &handshake.capabilities {
                if cap.len() > limits.max_capability_len {
                    return Err(ValidationError::CapabilityTooLong {
                        len: cap.len(),
                        limit: limits.max_capability_len,
                    });
                }
            }
        }
        Frame::HandshakeAck {
            server_capabilities,
            ..
        } => {
            if server_capabilities.len() > limits.max_capabilities {
                return Err(ValidationError::TooManyCapabilities {
                    count: server_capabilities.len(),
                    limit: limits.max_capabilities,
                });
            }
            for cap in server_capabilities {
                if cap.len() > limits.max_capability_len {
                    return Err(ValidationError::CapabilityTooLong {
                        len: cap.len(),
                        limit: limits.max_capability_len,
                    });
                }
            }
        }
        Frame::Data { data, .. } if data.len() > limits.max_payload_bytes => {
            return Err(ValidationError::PayloadTooLarge {
                size: data.len(),
                limit: limits.max_payload_bytes,
            });
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_limits_from_limits_config() {
        let limits = LimitsConfig {
            max_frame_bytes: 4096,
            max_token_len: 32,
            max_capabilities: 4,
            max_capability_len: 16,
            ..Default::default()
        };
        let validation = ValidationLimits::from(&limits);

        assert_eq!(validation.max_frame_bytes, 4096);
        assert_eq!(validation.max_payload_bytes, 4096);
        assert_eq!(validation.max_token_len, 32);
        assert_eq!(validation.max_capabilities, 4);
        assert_eq!(validation.max_capability_len, 16);
    }
}
