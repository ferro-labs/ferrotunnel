//! Frame validation for security hardening

use crate::frame::Frame;

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
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_token_len: 256,
            max_capabilities: 32,
            max_capability_len: 64,
            max_payload_bytes: 16 * 1024 * 1024,
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
