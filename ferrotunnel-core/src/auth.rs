//! Authentication utilities for secure token handling

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Constant-time comparison of two byte slices
/// Returns true if slices are equal, false otherwise
///
/// This prevents timing attacks where an attacker could determine
/// how many bytes match based on comparison time. The comparison runs
/// over `max(a.len(), b.len())` bytes and folds the length check into the
/// result, so it does not short-circuit on a length mismatch and therefore
/// does not leak the expected length through timing.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let max_len = a.len().max(b.len());
    let mut matches = a.len().ct_eq(&b.len());
    for index in 0..max_len {
        let left = a.get(index).copied().unwrap_or(0);
        let right = b.get(index).copied().unwrap_or(0);
        matches &= left.ct_eq(&right);
    }
    matches.into()
}

/// Hash a token using SHA-256
///
/// Useful for storing tokens securely - store the hash, compare against hash
#[must_use]
pub fn hash_token(token: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher.finalize().into()
}

/// Verify a token against a stored hash using constant-time comparison
#[must_use]
pub fn verify_token_hash(token: &str, expected_hash: &[u8; 32]) -> bool {
    let token_hash = hash_token(token);
    constant_time_eq(&token_hash, expected_hash)
}

/// Validate token format
///
/// Returns Ok(()) if token is valid, Err with reason if not
pub fn validate_token_format(token: &str, max_len: usize) -> Result<(), TokenValidationError> {
    if token.is_empty() {
        return Err(TokenValidationError::Empty);
    }
    if token.len() > max_len {
        return Err(TokenValidationError::TooLong {
            len: token.len(),
            max: max_len,
        });
    }
    if !token.chars().all(|c| c.is_ascii() && !c.is_ascii_control()) {
        return Err(TokenValidationError::InvalidCharacters);
    }
    Ok(())
}

/// Token validation errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum TokenValidationError {
    #[error("token is empty")]
    Empty,
    #[error("token too long: {len} bytes exceeds maximum of {max} bytes")]
    TooLong { len: usize, max: usize },
    #[error("token contains invalid characters")]
    InvalidCharacters,
}

impl From<TokenValidationError> for ferrotunnel_common::TunnelError {
    fn from(err: TokenValidationError) -> Self {
        ferrotunnel_common::TunnelError::Authentication(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_hash_and_verify() {
        let token = "my-secret-token";
        let hash = hash_token(token);

        assert!(verify_token_hash(token, &hash));
        assert!(!verify_token_hash("wrong-token", &hash));
    }

    #[test]
    fn test_validate_token_format() {
        assert!(validate_token_format("valid-token-123", 256).is_ok());
        assert!(validate_token_format("", 256).is_err());
        assert!(validate_token_format("x".repeat(300).as_str(), 256).is_err());
        assert!(validate_token_format("has\nnewline", 256).is_err());
    }
}
