//! Frame validation for security hardening

use crate::frame::{CloseReason, Frame, OpenStreamFrame};
use ferrotunnel_common::LimitsConfig;
use std::collections::HashMap;

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

    #[error("Too many entries: {count} exceeds limit of {limit}")]
    TooManyEntries { count: usize, limit: usize },

    #[error("Field too long: {len} bytes exceeds limit of {limit} bytes")]
    FieldTooLong { len: usize, limit: usize },

    #[error("Invalid version range: min_version {min} exceeds max_version {max}")]
    InvalidVersionRange { min: u8, max: u8 },
}

/// Default cardinality and per-element size floors for control-frame contents.
///
/// These are fixed security floors for this release rather than operator-tunable
/// knobs: `LimitsConfig` exposes only the frame/session budgets, and these
/// per-field bounds are derived from those plus the constants below. Promoting
/// them to `LimitsConfig` is deferred to the public-API-surface work in a later
/// milestone.
const DEFAULT_MAX_METADATA_ENTRIES: usize = 64;
const DEFAULT_MAX_NAME_LEN: usize = 256;
const DEFAULT_MAX_METADATA_VALUE_LEN: usize = 1024;
const DEFAULT_MAX_HEADERS: usize = 128;
const DEFAULT_MAX_HEADER_LEN: usize = 8 * 1024;
const DEFAULT_MAX_ERROR_MESSAGE_LEN: usize = 4 * 1024;
const DEFAULT_MAX_URL_LEN: usize = 2 * 1024;

/// Validation limits
///
/// The wire length-prefix already caps the encoded size of a frame at
/// `max_frame_bytes`. These limits additionally bound the *cardinality* and
/// *per-element size* of variable-length frame contents, so that a frame which
/// fits the byte budget cannot still force allocation amplification (for
/// example a `Register` packed with many tiny metadata entries).
///
/// The `max_frame_bytes`, `max_payload_bytes`, `max_token_len`,
/// `max_capabilities`, and `max_capability_len` fields track `LimitsConfig`; the
/// remaining per-field bounds are fixed defaults (see the `DEFAULT_MAX_*`
/// constants) applied uniformly regardless of the configured frame budget.
#[derive(Debug, Clone, Copy)]
pub struct ValidationLimits {
    pub max_frame_bytes: u64,
    pub max_token_len: usize,
    pub max_capabilities: usize,
    pub max_capability_len: usize,
    pub max_payload_bytes: usize,
    /// Maximum number of entries in a `Register` metadata map.
    pub max_metadata_entries: usize,
    /// Maximum byte length of a metadata key, a service name, or a plugin id.
    pub max_name_len: usize,
    /// Maximum byte length of a metadata value.
    pub max_metadata_value_len: usize,
    /// Maximum number of headers in an `OpenStream` frame.
    pub max_headers: usize,
    /// Maximum byte length of a single header name or value.
    pub max_header_len: usize,
    /// Maximum byte length of an `Error` frame message.
    pub max_error_message_len: usize,
    /// Maximum byte length of a public URL in a `RegisterAck` frame.
    pub max_url_len: usize,
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
            max_metadata_entries: DEFAULT_MAX_METADATA_ENTRIES,
            max_name_len: DEFAULT_MAX_NAME_LEN,
            max_metadata_value_len: DEFAULT_MAX_METADATA_VALUE_LEN,
            max_headers: DEFAULT_MAX_HEADERS,
            max_header_len: DEFAULT_MAX_HEADER_LEN,
            max_error_message_len: DEFAULT_MAX_ERROR_MESSAGE_LEN,
            max_url_len: DEFAULT_MAX_URL_LEN,
        }
    }
}

/// Reject a variable-length field whose byte length exceeds `limit`.
fn check_field_len(len: usize, limit: usize) -> Result<(), ValidationError> {
    if len > limit {
        return Err(ValidationError::FieldTooLong { len, limit });
    }
    Ok(())
}

/// Reject a collection whose entry count exceeds `limit`.
fn check_entry_count(count: usize, limit: usize) -> Result<(), ValidationError> {
    if count > limit {
        return Err(ValidationError::TooManyEntries { count, limit });
    }
    Ok(())
}

/// Validate a capability list shared by `Handshake` and `HandshakeAck`.
fn validate_capabilities(
    capabilities: &[String],
    limits: &ValidationLimits,
) -> Result<(), ValidationError> {
    if capabilities.len() > limits.max_capabilities {
        return Err(ValidationError::TooManyCapabilities {
            count: capabilities.len(),
            limit: limits.max_capabilities,
        });
    }
    for cap in capabilities {
        if cap.len() > limits.max_capability_len {
            return Err(ValidationError::CapabilityTooLong {
                len: cap.len(),
                limit: limits.max_capability_len,
            });
        }
    }
    Ok(())
}

fn validate_handshake(
    handshake: &crate::frame::HandshakeFrame,
    limits: &ValidationLimits,
) -> Result<(), ValidationError> {
    if handshake.token.len() > limits.max_token_len {
        return Err(ValidationError::TokenTooLong {
            len: handshake.token.len(),
            limit: limits.max_token_len,
        });
    }
    if let Some(tunnel_id) = &handshake.tunnel_id {
        check_field_len(tunnel_id.len(), limits.max_name_len)?;
    }
    if handshake.min_version > handshake.max_version {
        return Err(ValidationError::InvalidVersionRange {
            min: handshake.min_version,
            max: handshake.max_version,
        });
    }
    validate_capabilities(&handshake.capabilities, limits)
}

fn validate_register(
    service_name: &str,
    metadata: &HashMap<String, String>,
    limits: &ValidationLimits,
) -> Result<(), ValidationError> {
    check_field_len(service_name.len(), limits.max_name_len)?;
    check_entry_count(metadata.len(), limits.max_metadata_entries)?;
    for (key, value) in metadata {
        check_field_len(key.len(), limits.max_name_len)?;
        check_field_len(value.len(), limits.max_metadata_value_len)?;
    }
    Ok(())
}

fn validate_open_stream(
    open: &OpenStreamFrame,
    limits: &ValidationLimits,
) -> Result<(), ValidationError> {
    check_entry_count(open.headers.len(), limits.max_headers)?;
    for (name, value) in &open.headers {
        check_field_len(name.len(), limits.max_header_len)?;
        check_field_len(value.len(), limits.max_header_len)?;
    }
    Ok(())
}

fn validate_payload(len: usize, limits: &ValidationLimits) -> Result<(), ValidationError> {
    if len > limits.max_payload_bytes {
        return Err(ValidationError::PayloadTooLarge {
            size: len,
            limit: limits.max_payload_bytes,
        });
    }
    Ok(())
}

/// Validate the only variable-length `CloseReason`; other reasons are fixed-size.
fn validate_close_reason(
    reason: &CloseReason,
    limits: &ValidationLimits,
) -> Result<(), ValidationError> {
    match reason {
        CloseReason::Error(message) => check_field_len(message.len(), limits.max_error_message_len),
        CloseReason::Normal
        | CloseReason::Timeout
        | CloseReason::LocalServiceUnreachable
        | CloseReason::ProtocolViolation => Ok(()),
    }
}

/// Validate a decoded frame against limits.
///
/// The match is exhaustive (no wildcard) so that adding a `Frame` or
/// `CloseReason` variant fails to compile until a validation decision is made
/// here. Variants carrying variable-length content are bounded on cardinality
/// and per-element size; variants whose fields are all fixed-size accept
/// unconditionally.
pub fn validate_frame(frame: &Frame, limits: &ValidationLimits) -> Result<(), ValidationError> {
    match frame {
        Frame::Handshake(handshake) => validate_handshake(handshake, limits),
        Frame::HandshakeAck {
            server_capabilities,
            ..
        } => validate_capabilities(server_capabilities, limits),
        Frame::Register {
            service_name,
            metadata,
            ..
        } => validate_register(service_name, metadata, limits),
        Frame::RegisterAck { public_url, .. } => {
            check_field_len(public_url.len(), limits.max_url_len)
        }
        Frame::OpenStream(open) => validate_open_stream(open, limits),
        Frame::Data { data, .. } => validate_payload(data.len(), limits),
        Frame::CloseStream { reason, .. } => validate_close_reason(reason, limits),
        Frame::Error { message, .. } => {
            check_field_len(message.len(), limits.max_error_message_len)
        }
        Frame::PluginData { plugin_id, data } => {
            check_field_len(plugin_id.len(), limits.max_name_len)?;
            validate_payload(data.len(), limits)
        }
        Frame::StreamAck { .. } | Frame::Heartbeat { .. } | Frame::HeartbeatAck { .. } => Ok(()),
    }
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

    #[test]
    fn rejects_register_with_oversized_metadata_map() {
        let limits = ValidationLimits::default();
        let mut metadata = HashMap::new();
        for i in 0..=limits.max_metadata_entries {
            metadata.insert(format!("k{i}"), "v".to_string());
        }
        let frame = Frame::Register {
            service_name: "svc".into(),
            protocol: crate::frame::Protocol::HTTP,
            metadata,
        };
        assert!(matches!(
            validate_frame(&frame, &limits),
            Err(ValidationError::TooManyEntries { .. })
        ));
    }

    #[test]
    fn rejects_openstream_with_too_many_headers() {
        let limits = ValidationLimits::default();
        let headers = (0..=limits.max_headers)
            .map(|i| (format!("h{i}"), "v".to_string()))
            .collect();
        let frame = Frame::OpenStream(Box::new(OpenStreamFrame {
            stream_id: 1,
            protocol: crate::frame::Protocol::HTTP,
            headers,
            body_hint: None,
            priority: crate::frame::StreamPriority::default(),
        }));
        assert!(matches!(
            validate_frame(&frame, &limits),
            Err(ValidationError::TooManyEntries { .. })
        ));
    }

    #[test]
    fn accepts_well_formed_register() {
        let limits = ValidationLimits::default();
        let frame = Frame::Register {
            service_name: "svc".into(),
            protocol: crate::frame::Protocol::HTTP,
            metadata: HashMap::new(),
        };
        assert!(validate_frame(&frame, &limits).is_ok());
    }

    #[test]
    fn rejects_registerack_with_oversized_url() {
        let limits = ValidationLimits::default();
        let frame = Frame::RegisterAck {
            public_url: "x".repeat(limits.max_url_len + 1),
            status: crate::frame::RegisterStatus::Success,
        };
        assert!(matches!(
            validate_frame(&frame, &limits),
            Err(ValidationError::FieldTooLong { .. })
        ));
    }

    #[test]
    fn rejects_handshake_with_oversized_tunnel_id() {
        let limits = ValidationLimits::default();
        let frame = Frame::Handshake(Box::new(crate::frame::HandshakeFrame {
            token: "t".into(),
            tunnel_id: Some("x".repeat(limits.max_name_len + 1)),
            min_version: 1,
            max_version: 1,
            capabilities: Vec::new(),
        }));
        assert!(matches!(
            validate_frame(&frame, &limits),
            Err(ValidationError::FieldTooLong { .. })
        ));
    }

    #[test]
    fn rejects_handshake_with_inverted_version_range() {
        let limits = ValidationLimits::default();
        let frame = Frame::Handshake(Box::new(crate::frame::HandshakeFrame {
            token: "t".into(),
            tunnel_id: None,
            min_version: 2,
            max_version: 1,
            capabilities: Vec::new(),
        }));
        assert!(matches!(
            validate_frame(&frame, &limits),
            Err(ValidationError::InvalidVersionRange { min: 2, max: 1 })
        ));
    }

    #[test]
    fn rejects_close_stream_with_oversized_error_reason() {
        let limits = ValidationLimits::default();
        let frame = Frame::CloseStream {
            stream_id: 1,
            reason: CloseReason::Error("x".repeat(limits.max_error_message_len + 1)),
        };
        assert!(matches!(
            validate_frame(&frame, &limits),
            Err(ValidationError::FieldTooLong { .. })
        ));
    }

    #[test]
    fn accepts_close_stream_without_error_reason() {
        let limits = ValidationLimits::default();
        let frame = Frame::CloseStream {
            stream_id: 1,
            reason: CloseReason::Normal,
        };
        assert!(validate_frame(&frame, &limits).is_ok());
    }
}
