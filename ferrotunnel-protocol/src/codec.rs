//! Codec for encoding and decoding protocol frames
//!
//! Uses simple length-delimited framing for maximum performance:
//! - 4-byte length prefix (u32 big-endian)
//! - 1-byte type discriminator
//! - Variable payload
//!
//! This is similar to Rathole's approach and avoids COBS overhead.

use crate::constants::MAX_FRAME_SIZE;
use crate::frame::{Frame, ZeroCopyFrame};
use crate::validation::{validate_frame, ValidationLimits};
use bytes::{Buf, BufMut, BytesMut};
use ferrotunnel_common::LimitsConfig;
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Frame header size: 4 bytes length + 1 byte type
const HEADER_SIZE: usize = 5;
const MIN_MAX_FRAME_SIZE: usize = 1;

const FRAME_TYPE_CONTROL: u8 = 0x00;
const FRAME_TYPE_DATA: u8 = 0x01;
const FLAG_EOS: u8 = 0x01;

/// Tunnel protocol codec using length-delimited framing
///
/// Wire format:
/// ```text
/// ┌─────────────┬───────────┬──────────────┐
/// │ Length (u32)│ Type (u8) │ Payload      │
/// │ 4 bytes BE  │ 1 byte    │ N bytes      │
/// └─────────────┴───────────┴──────────────┘
/// ```
///
/// Length includes Type + Payload (not the length field itself).
///
/// Payload format depends on Type:
/// - Control (0x00): `bincode(Frame)` (excluding `Frame::Data`)
/// - Data (0x01): `[StreamID(u32)][Flags(u8)][Raw Bytes...]`
// `Copy` is required, not cosmetic: the tunnel client/server split a `Framed`
// via `into_parts()` and then read `parts.codec` more than once (for the split
// reader and for the batched sender). Keep this `Copy` — switching to `Clone`
// breaks that split-stream pattern.
#[derive(Debug, Clone, Copy)]
pub struct TunnelCodec {
    max_frame_size: usize,
    validation: ValidationLimits,
}

impl Default for TunnelCodec {
    fn default() -> Self {
        Self::with_max_frame_size(MAX_FRAME_SIZE as usize)
    }
}

impl TunnelCodec {
    /// Create a new codec instance with default max frame size
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enforce the wire-size contract shared by every constructor.
    ///
    /// # Panics
    ///
    /// Panics if `max_frame_size` is zero or does not fit in the `u32` wire
    /// length prefix. This is a programmer-error contract: configured limits
    /// coming from user input are validated (and rejected with an error) at the
    /// configuration boundary before reaching the codec.
    #[inline]
    fn assert_frame_size(max_frame_size: usize) {
        assert!(
            max_frame_size >= MIN_MAX_FRAME_SIZE,
            "max frame size must be greater than zero"
        );
        assert!(
            u32::try_from(max_frame_size).is_ok(),
            "max frame size must fit in the wire length prefix"
        );
    }

    /// Create a new codec instance with a custom max frame size.
    ///
    /// Only the byte budget (`max_frame_size`) is set from the argument; the
    /// per-field cardinality and element-size bounds keep their default floors.
    /// Use [`TunnelCodec::from_limits`] to derive both from a [`LimitsConfig`].
    ///
    /// # Panics
    ///
    /// Panics if `max_frame_size` is zero or does not fit in the `u32` wire
    /// length prefix. This is a programmer-error contract: user-supplied limits
    /// are validated and rejected with an error at the configuration boundary
    /// before reaching the codec.
    #[inline]
    pub fn with_max_frame_size(max_frame_size: usize) -> Self {
        Self::assert_frame_size(max_frame_size);
        Self {
            max_frame_size,
            validation: ValidationLimits {
                max_frame_bytes: max_frame_size as u64,
                max_payload_bytes: max_frame_size,
                ..ValidationLimits::default()
            },
        }
    }

    /// Create a new codec instance from configured resource limits.
    ///
    /// Both the byte budget and the per-field cardinality/element-size bounds
    /// enforced during [`Decoder::decode`] are derived from `limits`.
    ///
    /// # Panics
    ///
    /// Panics if `limits.max_frame_bytes` is zero or does not fit in the `u32`
    /// wire length prefix; see [`TunnelCodec::with_max_frame_size`].
    #[inline]
    pub fn from_limits(limits: &LimitsConfig) -> Self {
        let Ok(max_frame_size) = usize::try_from(limits.max_frame_bytes) else {
            panic!("max frame size must fit in usize")
        };
        Self::assert_frame_size(max_frame_size);
        Self {
            max_frame_size,
            validation: ValidationLimits::from(limits),
        }
    }

    /// Get the configured max frame size
    #[inline]
    pub fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    /// Decode as many complete frames as possible from `src`, appending to `out`.
    /// Returns the number of bytes consumed from `src`. Call with the same `BytesMut` and
    /// remaining bytes for incremental reading. Data frames are copied to owned `Frame`.
    pub fn decode_batch(
        &mut self,
        src: &mut BytesMut,
        out: &mut Vec<Frame>,
    ) -> Result<usize, io::Error> {
        let mut consumed = 0;
        loop {
            let before = src.len();
            match self.decode(src)? {
                Some(frame) => {
                    consumed += before - src.len();
                    out.push(frame);
                }
                None => break,
            }
        }
        Ok(consumed)
    }

    /// Parse data frames from `buf` without copying payload (zero-copy).
    /// Only complete data frames are returned; control frames are skipped (caller should use
    /// `decode` for mixed frames). Returns (bytes consumed, zero-copy data frames).
    pub fn decode_data_frames_zerocopy<'a>(
        &self,
        buf: &'a [u8],
        out: &mut Vec<ZeroCopyFrame<'a>>,
    ) -> Result<usize, io::Error> {
        let max_frame_size = self.max_frame_size;
        let mut offset = 0;
        while offset + HEADER_SIZE <= buf.len() {
            let length = u32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;
            if length == 0 || length > max_frame_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid frame length: {}", length),
                ));
            }
            let total_size = 4 + length;
            if buf.len() < offset + total_size {
                break;
            }
            let frame_type = buf[offset + 4];
            if frame_type == FRAME_TYPE_DATA {
                // type(1) + stream_id(4) + flags(1) = 6
                if length < 6 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Data frame payload too short",
                    ));
                }
                let stream_id = u32::from_be_bytes([
                    buf[offset + 5],
                    buf[offset + 6],
                    buf[offset + 7],
                    buf[offset + 8],
                ]);
                let flags = buf[offset + 9];
                let fin = (flags & FLAG_EOS) != 0;
                let data = &buf[offset + 10..offset + total_size];
                out.push(ZeroCopyFrame::Data {
                    stream_id,
                    data,
                    fin,
                });
            }
            offset += total_size;
        }
        Ok(offset)
    }
}

impl Decoder for TunnelCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least header (4 bytes length + 1 byte type)
        if src.len() < HEADER_SIZE {
            src.reserve(HEADER_SIZE - src.len());
            return Ok(None);
        }

        // Peek at length (don't consume yet)
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Validate frame size
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Frame length must be at least 1 byte",
            ));
        }
        if length > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Frame too large: {} bytes (max: {})",
                    length, self.max_frame_size
                ),
            ));
        }

        // Total frame size = 4 (length field) + length (type + payload)
        let total_size = 4 + length;
        if src.len() < total_size {
            // Not enough data yet, reserve space
            src.reserve(total_size - src.len());
            return Ok(None);
        }

        // Consume the frame
        let mut frame_bytes = src.split_to(total_size).freeze();
        frame_bytes.advance(4);

        // Parse type and payload
        let frame_type = frame_bytes.get_u8();

        match frame_type {
            FRAME_TYPE_DATA => {
                // Data frame: [StreamID(u32)][Flags(u8)][Raw Bytes...]
                if frame_bytes.remaining() < 5 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Data frame payload too short",
                    ));
                }
                let stream_id = frame_bytes.get_u32();
                let flags = frame_bytes.get_u8();
                let end_of_stream = (flags & FLAG_EOS) != 0;
                // Zero-copy slice of the remaining payload.
                let data = frame_bytes.split_to(frame_bytes.remaining());

                Ok(Some(Frame::Data {
                    stream_id,
                    data,
                    end_of_stream,
                }))
            }
            FRAME_TYPE_CONTROL => {
                // Control frame: bincode-encoded Frame. The bincode limit is a
                // compile-time absolute ceiling (`MAX_FRAME_SIZE`); runtime
                // enforcement is the length-prefix bound checked above plus the
                // per-variant validation below, which bounds the cardinality and
                // per-element size of variable-length frame contents.
                let config =
                    bincode_next::config::standard().with_limit::<{ MAX_FRAME_SIZE as usize }>();
                let (frame, _): (Frame, _) =
                    bincode_next::serde::decode_from_slice(frame_bytes.as_ref(), config).map_err(
                        |e| {
                            io::Error::new(io::ErrorKind::InvalidData, format!("Decode error: {e}"))
                        },
                    )?;
                validate_frame(&frame, &self.validation).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Frame validation: {e}"))
                })?;
                Ok(Some(frame))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown frame type: {frame_type}"),
            )),
        }
    }
}

impl Encoder<Frame> for TunnelCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match frame {
            Frame::Data {
                stream_id,
                data,
                end_of_stream,
            } => {
                // Data frame: [Length][Type][StreamID][Flags][Data]
                // Payload = type(1) + stream_id(4) + flags(1) + data.len()
                let payload_len = 1 + 4 + 1 + data.len();
                if payload_len > self.max_frame_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Frame too large: {} bytes (max: {})",
                            payload_len, self.max_frame_size
                        ),
                    ));
                }

                // Reserve space for entire frame
                dst.reserve(4 + payload_len);

                // Write length (type + payload, not including length field)
                dst.put_u32(payload_len as u32);
                // Write type
                dst.put_u8(FRAME_TYPE_DATA);
                // Write stream_id
                dst.put_u32(stream_id);
                // Write flags
                dst.put_u8(if end_of_stream { FLAG_EOS } else { 0 });
                // Write data directly - no copy needed if data is contiguous
                dst.extend_from_slice(&data);
            }
            control_frame => {
                // Control frame: [Length][Type][bincode payload]
                // First serialize the frame
                let config = bincode_next::config::standard();
                let serialized = bincode_next::serde::encode_to_vec(&control_frame, config)
                    .map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("Encode error: {e}"))
                    })?;

                let payload_len = 1 + serialized.len(); // type + serialized
                if payload_len > self.max_frame_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Frame too large: {} bytes (max: {})",
                            payload_len, self.max_frame_size
                        ),
                    ));
                }

                // Reserve and write
                dst.reserve(4 + payload_len);
                dst.put_u32(payload_len as u32);
                dst.put_u8(FRAME_TYPE_CONTROL);
                dst.extend_from_slice(&serialized);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_codec_round_trip() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        let frame = Frame::Heartbeat { timestamp: 12345 };

        // Encode
        codec.encode(frame.clone(), &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn decode_rejects_oversized_register_metadata() {
        // A `Register` that fits the frame-size budget but carries an abusive
        // number of metadata entries must be rejected by the decode path.
        let mut codec = TunnelCodec::new();
        let mut metadata = std::collections::HashMap::new();
        for i in 0..1000 {
            metadata.insert(format!("k{i}"), "v".to_string());
        }
        let frame = Frame::Register {
            service_name: "svc".into(),
            protocol: crate::frame::Protocol::HTTP,
            metadata,
        };

        let mut buf = BytesMut::new();
        codec
            .encode(frame, &mut buf)
            .expect("encode fits in frame size");

        let err = codec.decode(&mut buf).expect_err("decode must reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("validation"));
    }

    #[test]
    fn decode_rejects_oversized_openstream_headers() {
        // An `OpenStream` that fits the frame-size budget but carries an abusive
        // number of headers must be rejected by the decode path.
        let mut codec = TunnelCodec::new();
        let headers = (0..1000)
            .map(|i| (format!("h{i}"), "v".to_string()))
            .collect();
        let frame = Frame::OpenStream(Box::new(crate::frame::OpenStreamFrame {
            stream_id: 1,
            protocol: crate::frame::Protocol::HTTP,
            headers,
            body_hint: None,
            priority: crate::frame::StreamPriority::default(),
        }));

        let mut buf = BytesMut::new();
        codec
            .encode(frame, &mut buf)
            .expect("encode fits in frame size");

        let err = codec.decode(&mut buf).expect_err("decode must reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("validation"));
    }

    #[test]
    fn test_data_frame_round_trip() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        let frame = Frame::Data {
            stream_id: 42,
            data: Bytes::from("hello world"),
            end_of_stream: true,
        };

        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(frame, decoded);
    }

    #[test]
    fn test_partial_frame() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        let frame = Frame::Data {
            stream_id: 1,
            data: Bytes::from("hello world"),
            end_of_stream: false,
        };

        // Encode
        codec.encode(frame, &mut buf).unwrap();

        // Split the buffer
        let full_len = buf.len();
        let mut partial = buf.split_to(full_len / 2);

        // Try to decode partial frame - should return None
        let result = codec.decode(&mut partial);
        assert!(result.unwrap().is_none());

        // Unsplit and decode
        partial.unsplit(buf);
        let decoded = codec.decode(&mut partial).unwrap();
        assert!(decoded.is_some());
    }

    #[test]
    fn test_multiple_frames() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        let frames = vec![
            Frame::Heartbeat { timestamp: 1 },
            Frame::Heartbeat { timestamp: 2 },
            Frame::Heartbeat { timestamp: 3 },
        ];

        // Encode all frames
        for frame in &frames {
            codec.encode(frame.clone(), &mut buf).unwrap();
        }

        // Decode all frames
        for expected in &frames {
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(*expected, decoded);
        }

        // Buffer should be empty
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_max_frame_size() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        // Create a frame that's too large
        let large_data = vec![0u8; (MAX_FRAME_SIZE + 1) as usize];
        let frame = Frame::Data {
            stream_id: 1,
            data: Bytes::from(large_data),
            end_of_stream: false,
        };

        // Encoding should fail
        let result = codec.encode(frame, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_data_frame() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        // Test with a reasonably large frame (16KB)
        let data = vec![0xAB; 16 * 1024];
        let frame = Frame::Data {
            stream_id: 999,
            data: Bytes::from(data.clone()),
            end_of_stream: false,
        };

        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        if let Frame::Data {
            stream_id,
            data: decoded_data,
            end_of_stream,
        } = decoded
        {
            assert_eq!(stream_id, 999);
            assert_eq!(decoded_data.as_ref(), data.as_slice());
            assert!(!end_of_stream);
        } else {
            panic!("Expected Data frame");
        }
    }

    #[test]
    fn test_frame_size_validation_on_decode() {
        let mut codec = TunnelCodec::with_max_frame_size(100);
        let mut buf = BytesMut::new();

        // Craft a frame with invalid large length
        buf.put_u32(1000); // Length claims 1000 bytes
        buf.put_u8(FRAME_TYPE_DATA);
        buf.extend_from_slice(&[0u8; 10]); // Only 10 bytes of payload

        // Should fail validation before trying to read more
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "max frame size must be greater than zero")]
    fn test_zero_max_frame_size_rejected() {
        let _codec = TunnelCodec::with_max_frame_size(0);
    }

    #[test]
    fn test_from_limits_enforces_configured_frame_size() {
        let limits = LimitsConfig {
            max_frame_bytes: 8,
            ..Default::default()
        };
        let mut codec = TunnelCodec::from_limits(&limits);
        assert_eq!(codec.max_frame_size(), 8);
        let mut buf = BytesMut::new();

        let frame = Frame::Data {
            stream_id: 1,
            data: Bytes::from_static(b"abc"),
            end_of_stream: false,
        };

        let result = codec.encode(frame, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_length_frame_rejected() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();

        buf.put_u32(0); // Invalid length
        buf.put_u8(FRAME_TYPE_CONTROL);
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_batch() {
        let mut codec = TunnelCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(Frame::Heartbeat { timestamp: 1 }, &mut buf)
            .unwrap();
        codec
            .encode(Frame::Heartbeat { timestamp: 2 }, &mut buf)
            .unwrap();
        codec
            .encode(Frame::Heartbeat { timestamp: 3 }, &mut buf)
            .unwrap();
        let total_len = buf.len();

        let mut out = Vec::new();
        let consumed = codec.decode_batch(&mut buf, &mut out).unwrap();
        assert_eq!(consumed, total_len);
        assert_eq!(out.len(), 3);
        assert!(matches!(out[0], Frame::Heartbeat { timestamp: 1 }));
        assert!(matches!(out[1], Frame::Heartbeat { timestamp: 2 }));
        assert!(matches!(out[2], Frame::Heartbeat { timestamp: 3 }));
    }

    #[test]
    fn test_decode_data_frames_zerocopy() {
        use crate::frame::ZeroCopyFrame;
        let codec = TunnelCodec::new();
        let mut wire = BytesMut::new();
        wire.put_u32(6 + 4); // length: type(1)+stream_id(4)+flags(1)+data(4)=10, so 10
        wire.put_u8(FRAME_TYPE_DATA);
        wire.put_u32(42);
        wire.put_u8(FLAG_EOS);
        wire.extend_from_slice(b"abcd");

        let mut out = Vec::new();
        let consumed = codec
            .decode_data_frames_zerocopy(wire.as_ref(), &mut out)
            .unwrap();
        assert_eq!(consumed, 4 + 10);
        assert_eq!(out.len(), 1);
        match &out[0] {
            ZeroCopyFrame::Data {
                stream_id,
                data,
                fin,
            } => {
                assert_eq!(*stream_id, 42);
                assert_eq!(*data, b"abcd");
                assert!(*fin);
            }
        }
        let owned = out[0].to_owned();
        assert!(matches!(
            owned,
            Frame::Data {
                stream_id: 42,
                end_of_stream: true,
                ..
            }
        ));
    }
}
