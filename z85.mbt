///|
/// ZeroMQ RFC 32 (Z85) binary-to-text encoding, hexadecimal utilities, and
/// structured binary wire reader/writer cursors for ZMTP & CZMQ protocols.

///|
/// Official 85-character alphabet defined by ZeroMQ RFC 32 (Z85).
let z85_encoder_alphabet : String = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

///|
/// Build a 256-entry lookup table mapping ASCII byte values to Z85 base-85 digits (0..84),
/// or `-1` for characters outside the RFC 32 alphabet.
fn build_z85_decoder_table() -> FixedArray[Int32] {
  let table = FixedArray::make(256, -1)
  for i = 0; i < z85_encoder_alphabet.length(); i = i + 1 {
    let ch = z85_encoder_alphabet.code_unit_at(i).to_int() & 0xFF
    table[ch] = i
  }
  table
}

///|
let z85_decoder_table : FixedArray[Int32] = build_z85_decoder_table()

///|
/// Encode raw binary `data` into a ZeroMQ RFC 32 Z85 string.
///
/// Per RFC 32, the input byte length MUST be divisible by 4 (`len % 4 == 0`).
/// Every 4-byte big-endian binary frame is encoded into 5 printable ASCII characters.
/// Raises `ProtocolError` if `data.length() % 4 != 0`.
pub fn z85_encode(data : Bytes) -> String raise ZmqError {
  let len = data.length()
  if len % 4 != 0 {
    raise ProtocolError(
      "Z85 encode requires input byte length divisible by 4, got \{len}",
    )
  }
  if len == 0 {
    return ""
  }
  let sb = StringBuilder()
  let mut idx = 0
  while idx < len {
    let b0 = (data[idx].to_int() & 0xFF).to_int64()
    let b1 = (data[idx + 1].to_int() & 0xFF).to_int64()
    let b2 = (data[idx + 2].to_int() & 0xFF).to_int64()
    let b3 = (data[idx + 3].to_int() & 0xFF).to_int64()
    let mut value = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
    // Divisor powers: 85^4 = 52200625, 85^3 = 614125, 85^2 = 7225, 85^1 = 85, 85^0 = 1
    let d4 = (value / 52200625L).to_int()
    value = value % 52200625L
    let d3 = (value / 614125L).to_int()
    value = value % 614125L
    let d2 = (value / 7225L).to_int()
    value = value % 7225L
    let d1 = (value / 85L).to_int()
    let d0 = (value % 85L).to_int()
    sb.write_char(
      Int32::unsafe_to_char(z85_encoder_alphabet.code_unit_at(d4).to_int()),
    )
    sb.write_char(
      Int32::unsafe_to_char(z85_encoder_alphabet.code_unit_at(d3).to_int()),
    )
    sb.write_char(
      Int32::unsafe_to_char(z85_encoder_alphabet.code_unit_at(d2).to_int()),
    )
    sb.write_char(
      Int32::unsafe_to_char(z85_encoder_alphabet.code_unit_at(d1).to_int()),
    )
    sb.write_char(
      Int32::unsafe_to_char(z85_encoder_alphabet.code_unit_at(d0).to_int()),
    )
    idx = idx + 4
  }
  sb.to_string()
}

///|
/// Decode a ZeroMQ RFC 32 Z85 string back into raw binary `Bytes`.
///
/// Per RFC 32, the input string length MUST be divisible by 5 (`len % 5 == 0`),
/// all characters must belong to the Z85 alphabet, and each 5-character group must
/// decode to a 32-bit unsigned value (`<= 0xFFFFFFFF`).
pub fn z85_decode(encoded : String) -> Bytes raise ZmqError {
  let len = encoded.length()
  if len % 5 != 0 {
    raise ProtocolError(
      "Z85 decode requires input string length divisible by 5, got \{len}",
    )
  }
  if len == 0 {
    return b""
  }
  let out_cap = len / 5 * 4
  let out : Array[Byte] = Array(capacity=out_cap)
  let mut idx = 0
  while idx < len {
    let mut value = 0L
    for offset = 0; offset < 5; offset = offset + 1 {
      let code = encoded.code_unit_at(idx + offset).to_int()
      if code < 0 || code >= 256 {
        let pos = idx + offset
        raise ProtocolError(
          "Z85 decode encountered non-ASCII character at index \{pos}",
        )
      }
      let digit = z85_decoder_table[code]
      if digit < 0 {
        let pos = idx + offset
        raise ProtocolError(
          "Z85 decode encountered invalid character at index \{pos}",
        )
      }
      value = value * 85L + digit.to_int64()
    }
    if value > 0xFFFFFFFFL {
      raise ProtocolError(
        "Z85 decode 5-char block overflowed 32-bit unsigned integer at index \{idx}",
      )
    }
    out.push(((value >> 24) & 0xFFL).to_int().to_byte())
    out.push(((value >> 16) & 0xFFL).to_int().to_byte())
    out.push(((value >> 8) & 0xFFL).to_int().to_byte())
    out.push((value & 0xFFL).to_int().to_byte())
    idx = idx + 5
  }
  Bytes::from_array(out)
}

///|
/// Encode arbitrary-length binary `data` using Z85 with a 1-character padding header.
///
/// Prepends the PKCS-style pad count (`0`..`3` encoded as `'0'`..`'3'`) and pads `data`
/// with trailing zero bytes to a multiple of 4 before Z85 encoding.
pub fn z85_encode_padded(data : Bytes) -> String raise ZmqError {
  let len = data.length()
  let rem = len % 4
  let pad = if rem == 0 { 0 } else { 4 - rem }
  let padded : Array[Byte] = Array(capacity=len + pad)
  for i = 0; i < len; i = i + 1 {
    padded.push(data[i])
  }
  for p = 0; p < pad; p = p + 1 {
    padded.push(b'\x00')
  }
  let body = z85_encode(Bytes::from_array(padded))
  let prefix = Int32::unsafe_to_char(48 + pad).to_string()
  "\{prefix}\{body}"
}

///|
/// Decode a padded Z85 string produced by `z85_encode_padded` back into exact-length `Bytes`.
pub fn z85_decode_padded(encoded : String) -> Bytes raise ZmqError {
  if encoded.length() < 1 {
    raise ProtocolError("padded Z85 string cannot be empty")
  }
  let pad_code = encoded.code_unit_at(0).to_int()
  if pad_code < 48 || pad_code > 51 {
    raise ProtocolError("invalid Z85 padding header character")
  }
  let pad = pad_code - 48
  let sb = StringBuilder()
  for i = 1; i < encoded.length(); i = i + 1 {
    sb.write_char(Int32::unsafe_to_char(encoded.code_unit_at(i).to_int()))
  }
  let raw = z85_decode(sb.to_string())
  if pad > 0 && raw.length() < 4 {
    raise ProtocolError("truncated padded Z85 payload")
  }
  for i = raw.length() - pad; i < raw.length(); i = i + 1 {
    if raw[i] != b'\x00' {
      raise ProtocolError("non-zero trailing padding byte in padded Z85")
    }
  }
  slice_bytes(raw, 0, raw.length() - pad)
}

///|
/// Check whether a string is a valid canonical RFC 32 Z85 string.
pub fn is_valid_z85(s : String) -> Bool {
  let _ = z85_decode(s) catch { _ => return false }
  true
}

///|
let hex_lower_digits : String = "0123456789abcdef"

///|
let hex_upper_digits : String = "0123456789ABCDEF"

///|
/// Encode binary `data` as a hexadecimal string (lowercase by default).
pub fn bytes_to_hex(data : Bytes, uppercase~ : Bool = false) -> String {
  let digits = if uppercase { hex_upper_digits } else { hex_lower_digits }
  let sb = StringBuilder()
  for i = 0; i < data.length(); i = i + 1 {
    let b = data[i].to_int() & 0xFF
    let hi = (b >> 4) & 0x0F
    let lo = b & 0x0F
    sb.write_char(Int32::unsafe_to_char(digits.code_unit_at(hi).to_int()))
    sb.write_char(Int32::unsafe_to_char(digits.code_unit_at(lo).to_int()))
  }
  sb.to_string()
}

///|
fn hex_nibble_value(ch : Int32) -> Int32 {
  if ch >= 48 && ch <= 57 {
    ch - 48
  } else if ch >= 97 && ch <= 102 {
    ch - 97 + 10
  } else if ch >= 65 && ch <= 70 {
    ch - 65 + 10
  } else {
    -1
  }
}

///|
/// Decode a hexadecimal string (ignoring spaces, colons, and hyphens) into `Bytes`.
/// Raises `ProtocolError` on odd nibble counts or invalid characters.
pub fn hex_to_bytes(hex : String) -> Bytes raise ZmqError {
  let nibbles : Array[Int32] = Array(capacity=hex.length())
  for i = 0; i < hex.length(); i = i + 1 {
    let ch = hex.code_unit_at(i).to_int()
    if ch == 32 || ch == 9 || ch == 10 || ch == 13 || ch == 58 || ch == 45 {
      continue
    }
    let v = hex_nibble_value(ch)
    if v < 0 {
      raise ProtocolError("invalid hex character at index \{i}")
    }
    nibbles.push(v)
  }
  if nibbles.length() % 2 != 0 {
    let n = nibbles.length()
    raise ProtocolError("hex string has odd number of nibbles (\{n})")
  }
  let out : Array[Byte] = Array(capacity=nibbles.length() / 2)
  let mut idx = 0
  while idx < nibbles.length() {
    let byte_val = (nibbles[idx] << 4) | nibbles[idx + 1]
    out.push(byte_val.to_byte())
    idx = idx + 2
  }
  Bytes::from_array(out)
}

///|
/// Constant-time byte comparison to prevent timing side-channels when verifying
/// credentials, tokens, or message authentication tags.
pub fn constant_time_bytes_eq(a : Bytes, b : Bytes) -> Bool {
  if a.length() != b.length() {
    return false
  }
  let mut diff = 0
  for i = 0; i < a.length(); i = i + 1 {
    diff = diff | ((a[i].to_int() ^ b[i].to_int()) & 0xFF)
  }
  diff == 0
}

///|
/// Binary serialization buffer supporting network byte order (big-endian),
/// little-endian integers, ZMTP short strings (`OCTET` length), ZMTP long strings
/// (`4-OCTET` length), length-prefixed byte chunks, and key-value property tables.
pub struct WireWriter {
  buf : Array[Byte]
}

///|
/// Create a new `WireWriter` with optional pre-allocated byte `capacity`.
pub fn WireWriter::new(capacity~ : Int32 = 64) -> WireWriter {
  let cap = if capacity < 0 { 0 } else { capacity }
  { buf: Array(capacity=cap), }
}

///|
/// Current byte length written to this `WireWriter`.
pub fn WireWriter::length(self : WireWriter) -> Int32 {
  self.buf.length()
}

///|
/// Reset and clear the internal buffer of this `WireWriter`.
pub fn WireWriter::clear(self : WireWriter) -> Unit {
  self.buf.clear()
}

///|
/// Write a single unsigned 8-bit integer (`0..255`).
pub fn WireWriter::write_u8(self : WireWriter, val : Int32) -> Unit {
  self.buf.push((val & 0xFF).to_byte())
}

///|
/// Write a 16-bit integer in big-endian (network) byte order.
pub fn WireWriter::write_u16_be(self : WireWriter, val : Int32) -> Unit {
  self.buf.push(((val >> 8) & 0xFF).to_byte())
  self.buf.push((val & 0xFF).to_byte())
}

///|
/// Write a 16-bit integer in little-endian byte order.
pub fn WireWriter::write_u16_le(self : WireWriter, val : Int32) -> Unit {
  self.buf.push((val & 0xFF).to_byte())
  self.buf.push(((val >> 8) & 0xFF).to_byte())
}

///|
/// Write a 32-bit integer in big-endian (network) byte order.
pub fn WireWriter::write_i32_be(self : WireWriter, val : Int32) -> Unit {
  self.buf.push(((val >> 24) & 0xFF).to_byte())
  self.buf.push(((val >> 16) & 0xFF).to_byte())
  self.buf.push(((val >> 8) & 0xFF).to_byte())
  self.buf.push((val & 0xFF).to_byte())
}

///|
/// Write a 32-bit integer in little-endian byte order.
pub fn WireWriter::write_i32_le(self : WireWriter, val : Int32) -> Unit {
  self.buf.push((val & 0xFF).to_byte())
  self.buf.push(((val >> 8) & 0xFF).to_byte())
  self.buf.push(((val >> 16) & 0xFF).to_byte())
  self.buf.push(((val >> 24) & 0xFF).to_byte())
}

///|
/// Write a 64-bit integer in big-endian (network) byte order.
pub fn WireWriter::write_i64_be(self : WireWriter, val : Int64) -> Unit {
  self.buf.push(((val >> 56) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 48) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 40) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 32) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 24) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 16) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 8) & 0xFFL).to_int().to_byte())
  self.buf.push((val & 0xFFL).to_int().to_byte())
}

///|
/// Write a 64-bit integer in little-endian byte order.
pub fn WireWriter::write_i64_le(self : WireWriter, val : Int64) -> Unit {
  self.buf.push((val & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 8) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 16) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 24) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 32) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 40) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 48) & 0xFFL).to_int().to_byte())
  self.buf.push(((val >> 56) & 0xFFL).to_int().to_byte())
}

///|
/// Append raw `bytes` without any length prefix.
pub fn WireWriter::write_raw_bytes(self : WireWriter, bytes : Bytes) -> Unit {
  for i = 0; i < bytes.length(); i = i + 1 {
    self.buf.push(bytes[i])
  }
}

///|
/// Write a ZMTP short string (`1-byte length` + up to 255 ASCII/UTF-8 bytes).
/// Raises `ProtocolError` if string length exceeds 255 bytes.
pub fn WireWriter::write_short_string(
  self : WireWriter,
  s : String,
) -> Unit raise ZmqError {
  let raw = string_to_ascii_bytes(s)
  if raw.length() > 255 {
    let n = raw.length()
    raise ProtocolError("ZMTP short string exceeds 255 bytes (got \{n})")
  }
  self.write_u8(raw.length())
  self.write_raw_bytes(raw)
}

///|
/// Write a ZMTP long string (`4-byte big-endian length` + string bytes).
pub fn WireWriter::write_long_string(self : WireWriter, s : String) -> Unit {
  let raw = string_to_ascii_bytes(s)
  self.write_i32_be(raw.length())
  self.write_raw_bytes(raw)
}

///|
/// Write a length-prefixed binary chunk (`4-byte big-endian length` + raw bytes).
pub fn WireWriter::write_chunk(self : WireWriter, chunk : Bytes) -> Unit {
  self.write_i32_be(chunk.length())
  self.write_raw_bytes(chunk)
}

///|
/// Write a key-value dictionary in ZMTP metadata format (`1-byte key len` + key +
/// `4-byte big-endian val len` + val), preceded by a `4-byte entry count`.
pub fn WireWriter::write_string_map(
  self : WireWriter,
  entries : Map[String, String],
) -> Unit raise ZmqError {
  let pairs : Array[(String, String)] = []
  for k, v in entries {
    pairs.push((k, v))
  }
  self.write_i32_be(pairs.length())
  for pair in pairs {
    let (k, v) = pair
    self.write_short_string(k)
    self.write_long_string(v)
  }
}

///|
/// Finalize and return an immutable `Bytes` snapshot of the written payload.
pub fn WireWriter::to_bytes(self : WireWriter) -> Bytes {
  Bytes::from_array(self.buf)
}

///|
/// Finalize and wrap the written payload as a single-frame `Message`.
pub fn WireWriter::to_message(self : WireWriter) -> Message {
  Message::from_bytes(self.to_bytes())
}

///|
/// Zero-copy binary deserialization cursor over an immutable `Bytes` buffer.
pub struct WireReader {
  data : Bytes
  mut pos : Int32
}

///|
/// Create a new `WireReader` positioned at offset `0` of `data`.
pub fn WireReader::new(data : Bytes) -> WireReader {
  { data, pos: 0, }
}

///|
/// Current byte offset of the reader cursor.
pub fn WireReader::position(self : WireReader) -> Int32 {
  self.pos
}

///|
/// Number of unread bytes remaining in the buffer.
pub fn WireReader::remaining(self : WireReader) -> Int32 {
  self.data.length() - self.pos
}

///|
/// Whether the reader has consumed all bytes in the buffer.
pub fn WireReader::is_eof(self : WireReader) -> Bool {
  self.pos >= self.data.length()
}

///|
fn WireReader::ensure_available(
  self : WireReader,
  need : Int32,
) -> Unit raise ZmqError {
  if need < 0 || self.pos + need > self.data.length() {
    let rem = self.remaining()
    raise ProtocolError(
      "WireReader buffer underflow: needed \{need} bytes, only \{rem} remaining",
    )
  }
}

///|
/// Read an unsigned 8-bit integer (`0..255`).
pub fn WireReader::read_u8(self : WireReader) -> Int32 raise ZmqError {
  self.ensure_available(1)
  let v = self.data[self.pos].to_int() & 0xFF
  self.pos = self.pos + 1
  v
}

///|
/// Read an unsigned 16-bit integer in big-endian byte order (`0..65535`).
pub fn WireReader::read_u16_be(self : WireReader) -> Int32 raise ZmqError {
  self.ensure_available(2)
  let b0 = self.data[self.pos].to_int() & 0xFF
  let b1 = self.data[self.pos + 1].to_int() & 0xFF
  self.pos = self.pos + 2
  (b0 << 8) | b1
}

///|
/// Read an unsigned 16-bit integer in little-endian byte order (`0..65535`).
pub fn WireReader::read_u16_le(self : WireReader) -> Int32 raise ZmqError {
  self.ensure_available(2)
  let b0 = self.data[self.pos].to_int() & 0xFF
  let b1 = self.data[self.pos + 1].to_int() & 0xFF
  self.pos = self.pos + 2
  b0 | (b1 << 8)
}

///|
/// Read a signed 32-bit integer in big-endian byte order.
pub fn WireReader::read_i32_be(self : WireReader) -> Int32 raise ZmqError {
  self.ensure_available(4)
  let b0 = self.data[self.pos].to_int() & 0xFF
  let b1 = self.data[self.pos + 1].to_int() & 0xFF
  let b2 = self.data[self.pos + 2].to_int() & 0xFF
  let b3 = self.data[self.pos + 3].to_int() & 0xFF
  self.pos = self.pos + 4
  (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
}

///|
/// Read a signed 32-bit integer in little-endian byte order.
pub fn WireReader::read_i32_le(self : WireReader) -> Int32 raise ZmqError {
  self.ensure_available(4)
  let b0 = self.data[self.pos].to_int() & 0xFF
  let b1 = self.data[self.pos + 1].to_int() & 0xFF
  let b2 = self.data[self.pos + 2].to_int() & 0xFF
  let b3 = self.data[self.pos + 3].to_int() & 0xFF
  self.pos = self.pos + 4
  b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
}

///|
/// Read a signed 64-bit integer in big-endian byte order.
pub fn WireReader::read_i64_be(self : WireReader) -> Int64 raise ZmqError {
  self.ensure_available(8)
  let mut acc = 0L
  for i = 0; i < 8; i = i + 1 {
    let b = (self.data[self.pos + i].to_int() & 0xFF).to_int64()
    acc = (acc << 8) | b
  }
  self.pos = self.pos + 8
  acc
}

///|
/// Read a signed 64-bit integer in little-endian byte order.
pub fn WireReader::read_i64_le(self : WireReader) -> Int64 raise ZmqError {
  self.ensure_available(8)
  let mut acc = 0L
  for i = 0; i < 8; i = i + 1 {
    let b = (self.data[self.pos + i].to_int() & 0xFF).to_int64()
    let shift = i * 8
    acc = acc | (b << shift)
  }
  self.pos = self.pos + 8
  acc
}

///|
/// Read an exact slice of `len` bytes from the current position.
pub fn WireReader::read_raw_bytes(
  self : WireReader,
  len : Int32,
) -> Bytes raise ZmqError {
  if len < 0 {
    raise ProtocolError("WireReader negative byte length \{len}")
  }
  self.ensure_available(len)
  let out = slice_bytes(self.data, self.pos, self.pos + len)
  self.pos = self.pos + len
  out
}

///|
/// Read a ZMTP short string (`1-byte length` + string bytes).
pub fn WireReader::read_short_string(
  self : WireReader,
) -> String raise ZmqError {
  let len = self.read_u8()
  let raw = self.read_raw_bytes(len)
  ascii_bytes_to_string(raw)
}

///|
/// Read a ZMTP long string (`4-byte big-endian length` + string bytes).
pub fn WireReader::read_long_string(self : WireReader) -> String raise ZmqError {
  let len = self.read_i32_be()
  if len < 0 || len > DEFAULT_MAX_FRAME_SIZE {
    raise ProtocolError("WireReader invalid long string length \{len}")
  }
  let raw = self.read_raw_bytes(len)
  ascii_bytes_to_string(raw)
}

///|
/// Read a length-prefixed binary chunk (`4-byte big-endian length` + raw bytes).
pub fn WireReader::read_chunk(self : WireReader) -> Bytes raise ZmqError {
  let len = self.read_i32_be()
  if len < 0 || len > DEFAULT_MAX_FRAME_SIZE {
    raise ProtocolError("WireReader invalid chunk length \{len}")
  }
  self.read_raw_bytes(len)
}

///|
/// Read a string dictionary encoded by `WireWriter::write_string_map`.
pub fn WireReader::read_string_map(
  self : WireReader,
) -> Map[String, String] raise ZmqError {
  let count = self.read_i32_be()
  if count < 0 || count > 65536 {
    raise ProtocolError("WireReader invalid string map count \{count}")
  }
  let out : Map[String, String] = Map([])
  for i = 0; i < count; i = i + 1 {
    let k = self.read_short_string()
    let v = self.read_long_string()
    out[k] = v
  }
  out
}

///|
/// Pack an arbitrary multipart `Message` into a single self-contained binary blob
/// (`4-byte magic "ZMSG"` + `4-byte frame count` + length-prefixed frames) suitable for
/// persistence, caching (LVC), or tunneling over single-frame transports.
pub fn pack_multipart_blob(msg : Message) -> Bytes {
  let w = WireWriter::new(capacity=msg.total_bytes() + 16)
  w.write_raw_bytes(b"ZMSG")
  w.write_i32_be(msg.frames.length())
  for f in msg.frames {
    w.write_chunk(f)
  }
  w.to_bytes()
}

///|
/// Unpack a binary blob created by `pack_multipart_blob` back into a multipart `Message`.
pub fn unpack_multipart_blob(blob : Bytes) -> Message raise ZmqError {
  let r = WireReader::new(blob)
  let magic = r.read_raw_bytes(4)
  if magic != b"ZMSG" {
    raise ProtocolError("invalid multipart blob magic header")
  }
  let count = r.read_i32_be()
  if count < 0 || count > 65536 {
    raise ProtocolError("invalid multipart blob frame count \{count}")
  }
  let frames : Array[Bytes] = Array(capacity=count)
  for i = 0; i < count; i = i + 1 {
    frames.push(r.read_chunk())
  }
  if !r.is_eof() {
    raise ProtocolError("trailing garbage bytes after multipart blob")
  }
  Message::multipart(frames)
}
