///|
/// ZMTP 3.1 error types covering protocol violations, security failures,
/// socket state errors, routing issues, and transport errors.
pub(all) suberror ZmqError {
  ProtocolError(String)
  FrameTooLarge(Int32)
  InvalidSocketPair(String, String)
  AuthFailed(String)
  InvalidState(String)
  InvalidEndpoint(String)
  Unroutable(Bytes)
  HighWaterMarkReached
  Closed
  IoError(String)
} derive(Eq, Debug)

///|
pub extend ZmqError with Eq::{not_equal, equal}

///|
pub extend ZmqError with @debug.Debug::{to_repr}

///|
pub extend ZmqError with Show::{to_string, output}

///|
pub impl Show for ZmqError with output(self, logger) {
  match self {
    ProtocolError(msg) => logger.write_string("ProtocolError(\{msg})")
    FrameTooLarge(sz) => logger.write_string("FrameTooLarge(\{sz})")
    InvalidSocketPair(l, r) =>
      logger.write_string("InvalidSocketPair(\{l}, \{r})")
    AuthFailed(msg) => logger.write_string("AuthFailed(\{msg})")
    InvalidState(msg) => logger.write_string("InvalidState(\{msg})")
    InvalidEndpoint(msg) => logger.write_string("InvalidEndpoint(\{msg})")
    Unroutable(_) => logger.write_string("Unroutable")
    HighWaterMarkReached => logger.write_string("HighWaterMarkReached")
    Closed => logger.write_string("Closed")
    IoError(msg) => logger.write_string("IoError(\{msg})")
  }
}


///|
/// Frame flag bits defined by ZMTP 3.1 (RFC 23 Section 3.1).
pub const FLAG_MORE : Int32 = 0x01

///|
pub const FLAG_LONG : Int32 = 0x02

///|
pub const FLAG_COMMAND : Int32 = 0x04

///|
/// Bits 3..7 of the frame flags octet are reserved and MUST be zero.
pub const FLAG_RESERVED_MASK : Int32 = 0xF8

///|
/// Default maximum allowed single frame payload size (64 MiB).
pub const DEFAULT_MAX_FRAME_SIZE : Int32 = 67108864

///|
/// Convert an ASCII string into raw bytes.
pub fn string_to_ascii_bytes(s : String) -> Bytes {
  let out : Array[Byte] = Array(capacity=s.length())
  for i = 0; i < s.length(); i = i + 1 {
    out.push((s.code_unit_at(i).to_int() & 0xFF).to_byte())
  }
  Bytes::from_array(out)
}

///|
/// Convert raw ASCII/Latin-1 bytes into a String.
pub fn ascii_bytes_to_string(b : Bytes) -> String {
  let sb = StringBuilder()
  for i = 0; i < b.length(); i = i + 1 {
    sb.write_char(Int32::unsafe_to_char(b[i].to_int()))
  }
  sb.to_string()
}

///|
/// Slice a sub-range `[start, end)` from `Bytes`.
pub fn slice_bytes(b : Bytes, start : Int32, end : Int32) -> Bytes {
  let len = if end > start { end - start } else { 0 }
  let out : Array[Byte] = Array(capacity=len)
  for i = start; i < end; i = i + 1 {
    out.push(b[i])
  }
  Bytes::from_array(out)
}

///|
/// Concatenate an array of `Bytes` into a single `Bytes`.
pub fn concat_bytes(parts : Array[Bytes]) -> Bytes {
  let mut total = 0
  for p in parts {
    total = total + p.length()
  }
  let out : Array[Byte] = Array(capacity=total)
  for p in parts {
    for i = 0; i < p.length(); i = i + 1 {
      out.push(p[i])
    }
  }
  Bytes::from_array(out)
}

///|
/// Check whether `haystack` starts with `prefix`.
pub fn bytes_starts_with(haystack : Bytes, prefix : Bytes) -> Bool {
  if prefix.length() > haystack.length() {
    return false
  }
  for i = 0; i < prefix.length(); i = i + 1 {
    if haystack[i] != prefix[i] {
      return false
    }
  }
  true
}

///|
/// A single ZMTP 3.1 wire frame (either a message data frame or a command frame).
pub struct Frame {
  more : Bool
  is_command : Bool
  data : Bytes
} derive(Eq, Debug)

///|
pub extend Frame with Eq::{not_equal, equal}

///|
pub extend Frame with @debug.Debug::{to_repr}

///|
/// Create a data frame with an optional `more` indicator.
pub fn Frame::data_frame(data : Bytes, more~ : Bool = false) -> Frame {
  { more, is_command: false, data, }
}

///|
/// Create a command frame (`COMMAND` flag set, `MORE` flag cleared).
pub fn Frame::command_frame(data : Bytes) -> Frame {
  { more: false, is_command: true, data, }
}

///|
/// Encode a single ZMTP 3.1 frame into wire bytes.
/// Uses short framing (1-byte size) for payloads <= 255 bytes and
/// long framing (8-byte big-endian size) for payloads > 255 bytes.
pub fn Frame::encode(self : Frame) -> Bytes {
  let len = self.data.length()
  let mut flags = 0
  if self.more && !self.is_command {
    flags = flags | FLAG_MORE
  }
  if self.is_command {
    flags = flags | FLAG_COMMAND
  }
  let is_long = len > 255
  if is_long {
    flags = flags | FLAG_LONG
  }
  let hdr_len = if is_long { 9 } else { 2 }
  let out : Array[Byte] = Array(capacity=hdr_len + len)
  out.push(flags.to_byte())
  if is_long {
    // 64-bit unsigned big-endian length. Since `len` fits in a 32-bit positive Int,
    // the upper 4 octets are zero.
    out.push(b'\x00')
    out.push(b'\x00')
    out.push(b'\x00')
    out.push(b'\x00')
    out.push(((len >> 24) & 0xFF).to_byte())
    out.push(((len >> 16) & 0xFF).to_byte())
    out.push(((len >> 8) & 0xFF).to_byte())
    out.push((len & 0xFF).to_byte())
  } else {
    out.push((len & 0xFF).to_byte())
  }
  for i = 0; i < len; i = i + 1 {
    out.push(self.data[i])
  }
  Bytes::from_array(out)
}

///|
/// A ZeroMQ multipart message consisting of one or more contiguous data frames.
pub struct Message {
  frames : Array[Bytes]
} derive(Eq, Debug)

///|
pub extend Message with Eq::{not_equal, equal}

///|
pub extend Message with @debug.Debug::{to_repr}

///|
/// Construct a single-frame message from raw bytes.
pub fn Message::from_bytes(payload : Bytes) -> Message {
  { frames: [payload], }
}

///|
/// Construct a single-frame message from an ASCII/UTF-8 string.
pub fn Message::from_string(s : String) -> Message {
  { frames: [string_to_ascii_bytes(s)], }
}

///|
/// Construct a multipart message from an array of frame byte payloads.
/// If an empty array is passed, a single empty frame `[b""]` is used per ZMTP rules.
pub fn Message::multipart(frames : Array[Bytes]) -> Message {
  if frames.is_empty() {
    { frames: [b""], }
  } else {
    { frames, }
  }
}

///|
/// Number of frames in the multipart message.
pub fn Message::len(self : Message) -> Int32 {
  self.frames.length()
}

///|
/// Return the first frame's bytes (or empty bytes if none).
pub fn Message::first_bytes(self : Message) -> Bytes {
  if self.frames.is_empty() {
    b""
  } else {
    self.frames[0]
  }
}

///|
/// Return the first frame decoded as a string.
pub fn Message::first_string(self : Message) -> String {
  ascii_bytes_to_string(self.first_bytes())
}

///|
/// Encode a multipart message into ZMTP 3.1 wire bytes, setting `MORE=1` on all
/// frames except the final frame.
pub fn Message::encode(self : Message) -> Bytes {
  let parts = if self.frames.is_empty() { [b""] } else { self.frames }
  let encoded : Array[Bytes] = Array(capacity=parts.length())
  let n = parts.length()
  for i = 0; i < n; i = i + 1 {
    let f = Frame::data_frame(parts[i], more=i < n - 1)
    encoded.push(f.encode())
  }
  concat_bytes(encoded)
}

///|
/// Output items decoded from the ZMTP frame stream.
pub(all) enum DecodedItem {
  CommandFrame(Bytes)
  DataMessage(Message)
} derive(Eq, Debug)

///|
pub extend DecodedItem with Eq::{not_equal, equal}

///|
pub extend DecodedItem with @debug.Debug::{to_repr}

///|
/// Streaming ZMTP 3.1 frame and multipart message decoder.
/// Handles arbitrary TCP fragmentation (partial headers/bodies) and coalescing.
pub struct FrameDecoder {
  max_frame_size : Int32
  mut buf : Array[Byte]
  mut pos : Int32
  mut pending_frames : Array[Bytes]
  mut failed : Bool
}

///|
/// Create a new streaming frame decoder with an optional maximum frame size cap.
pub fn FrameDecoder::new(
  max_frame_size~ : Int32 = DEFAULT_MAX_FRAME_SIZE,
) -> FrameDecoder {
  { max_frame_size, buf: [], pos: 0, pending_frames: [], failed: false, }
}

///|
/// Feed raw bytes into the streaming decoder and return all completed frames/messages.
pub fn FrameDecoder::feed(
  self : FrameDecoder,
  chunk : Bytes,
) -> Array[DecodedItem] raise ZmqError {
  if self.failed {
    raise ProtocolError("decoder is in failed state due to prior error")
  }
  for i = 0; i < chunk.length(); i = i + 1 {
    self.buf.push(chunk[i])
  }
  let out : Array[DecodedItem] = []
  while true {
    let avail = self.buf.length() - self.pos
    if avail < 1 {
      break
    }
    let flags = self.buf[self.pos].to_int()
    if (flags & FLAG_RESERVED_MASK) != 0 {
      self.failed = true
      raise ProtocolError("non-zero reserved bits in frame flags: \{flags}")
    }
    let is_more = (flags & FLAG_MORE) != 0
    let is_long = (flags & FLAG_LONG) != 0
    let is_command = (flags & FLAG_COMMAND) != 0
    if is_command && is_more {
      self.failed = true
      raise ProtocolError("COMMAND frame MUST NOT have MORE flag set")
    }
    let hdr_len = if is_long { 9 } else { 2 }
    if avail < hdr_len {
      break
    }
    let mut body_len = 0
    if is_long {
      // Check upper 4 bytes of 64-bit big-endian length first
      let hi = (self.buf[self.pos + 1].to_int() << 24) |
        (self.buf[self.pos + 2].to_int() << 16) |
        (self.buf[self.pos + 3].to_int() << 8) |
        self.buf[self.pos + 4].to_int()
      let lo = (self.buf[self.pos + 5].to_int() << 24) |
        (self.buf[self.pos + 6].to_int() << 16) |
        (self.buf[self.pos + 7].to_int() << 8) |
        self.buf[self.pos + 8].to_int()
      if hi != 0 || lo < 0 || lo > self.max_frame_size {
        self.failed = true
        raise FrameTooLarge(lo)
      }
      body_len = lo
    } else {
      body_len = self.buf[self.pos + 1].to_int()
      if body_len > self.max_frame_size {
        self.failed = true
        raise FrameTooLarge(body_len)
      }
    }
    if avail < hdr_len + body_len {
      break
    }
    let payload : Array[Byte] = Array(capacity=body_len)
    let start = self.pos + hdr_len
    for i = 0; i < body_len; i = i + 1 {
      payload.push(self.buf[start + i])
    }
    self.pos = start + body_len
    let payload_bytes = Bytes::from_array(payload)
    if is_command {
      out.push(CommandFrame(payload_bytes))
    } else {
      self.pending_frames.push(payload_bytes)
      if !is_more {
        let msg = Message::multipart(self.pending_frames)
        self.pending_frames = []
        out.push(DataMessage(msg))
      }
    }
  }
  if self.pos > 0 {
    if self.pos == self.buf.length() {
      self.buf = []
      self.pos = 0
    } else if self.pos >= 4096 {
      let rest : Array[Byte] = Array(capacity=self.buf.length() - self.pos)
      for i = self.pos; i < self.buf.length(); i = i + 1 {
        rest.push(self.buf[i])
      }
      self.buf = rest
      self.pos = 0
    }
  }
  out
}
