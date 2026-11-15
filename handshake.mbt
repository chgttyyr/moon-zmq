///|
/// Standard ZeroMQ socket types defined across RFC 28, 29, 30, and 31.
pub(all) enum SocketType {
  Req
  Rep
  Dealer
  Router
  Pub
  Sub
  XPub
  XSub
  Push
  Pull
  Pair
} derive(Eq, Debug)

///|
pub extend SocketType with Eq::{not_equal, equal}

///|
pub extend SocketType with @debug.Debug::{to_repr}

///|
/// Return the canonical ZMTP 3.1 `Socket-Type` string for this socket type.
pub fn SocketType::to_zmtp_name(self : SocketType) -> String {
  match self {
    Req => "REQ"
    Rep => "REP"
    Dealer => "DEALER"
    Router => "ROUTER"
    Pub => "PUB"
    Sub => "SUB"
    XPub => "XPUB"
    XSub => "XSUB"
    Push => "PUSH"
    Pull => "PULL"
    Pair => "PAIR"
  }
}

///|
/// Convert an ASCII string to uppercase.
pub fn ascii_to_upper(s : String) -> String {
  let sb = StringBuilder()
  for i = 0; i < s.length(); i = i + 1 {
    let c = s.code_unit_at(i).to_int()
    let up = if c >= 97 && c <= 122 { c - 32 } else { c }
    sb.write_char(Int::unsafe_to_char(up))
  }
  sb.to_string()
}

///|
/// Parse a ZMTP `Socket-Type` name into a `SocketType`.
pub fn SocketType::from_zmtp_name(name : String) -> SocketType? {
  match ascii_to_upper(name) {
    "REQ" => Some(Req)
    "REP" => Some(Rep)
    "DEALER" => Some(Dealer)
    "ROUTER" => Some(Router)
    "PUB" => Some(Pub)
    "SUB" => Some(Sub)
    "XPUB" => Some(XPub)
    "XSUB" => Some(XSub)
    "PUSH" => Some(Push)
    "PULL" => Some(Pull)
    "PAIR" => Some(Pair)
    _ => None
  }
}

///|
/// Check whether two socket types are valid peers according to ZeroMQ RFC 28/29/30/31.
pub fn SocketType::is_compatible_with(
  self : SocketType,
  peer : SocketType,
) -> Bool {
  match (self, peer) {
    (Req, Rep) | (Req, Router) => true
    (Rep, Req) | (Rep, Dealer) => true
    (Dealer, Rep) | (Dealer, Dealer) | (Dealer, Router) => true
    (Router, Req) | (Router, Dealer) | (Router, Router) => true
    (Pub, Sub) | (Pub, XSub) => true
    (XPub, Sub) | (XPub, XSub) => true
    (Sub, Pub) | (Sub, XPub) => true
    (XSub, Pub) | (XSub, XPub) => true
    (Push, Pull) => true
    (Pull, Push) => true
    (Pair, Pair) => true
    _ => false
  }
}

///|
/// 64-byte ZMTP 3.1 Greeting exchanged at the beginning of a connection (RFC 23 Section 3.2).
pub struct Greeting {
  version_major : Int
  version_minor : Int
  mechanism : String
  as_server : Bool
} derive(Eq, Debug)

///|
pub extend Greeting with Eq::{not_equal, equal}

///|
pub extend Greeting with @debug.Debug::{to_repr}

///|
/// Construct a ZMTP 3.1 Greeting.
pub fn Greeting::new(
  mechanism? : String = "NULL",
  as_server? : Bool = false,
  version_major? : Int = 3,
  version_minor? : Int = 1,
) -> Greeting {
  { version_major, version_minor, mechanism, as_server }
}

///|
/// Encode the 64-byte ZMTP Greeting into wire bytes.
pub fn Greeting::encode(self : Greeting) -> Bytes {
  let out : Array[Byte] = Array(capacity=64)
  // Signature: 0xFF + 8 padding octets + 0x7F
  out.push(b'\xFF')
  for i = 0; i < 8; i = i + 1 {
    // Set last padding octet to 0x01 for libzmq compatibility; ZMTP 3.1 peers ignore padding.
    out.push(if i == 7 { b'\x01' } else { b'\x00' })
  }
  out.push(b'\x7F')
  // Version: major (3), minor (1)
  out.push((self.version_major & 0xFF).to_byte())
  out.push((self.version_minor & 0xFF).to_byte())
  // Mechanism: 20 bytes ASCII, trailing zeroes
  let mlen = self.mechanism.length()
  for i = 0; i < 20; i = i + 1 {
    if i < mlen {
      out.push((self.mechanism.code_unit_at(i).to_int() & 0xFF).to_byte())
    } else {
      out.push(b'\x00')
    }
  }
  // as-server: 1 byte (0x00 or 0x01)
  out.push(if self.as_server { b'\x01' } else { b'\x00' })
  // filler: 31 zero bytes
  for _i = 0; _i < 31; _i = _i + 1 {
    out.push(b'\x00')
  }
  Bytes::from_array(out)
}

///|
/// Decode and validate a 64-byte ZMTP Greeting.
pub fn Greeting::decode(raw : Bytes) -> Greeting raise ZmqError {
  if raw.length() != 64 {
    raise ProtocolError("greeting must be exactly 64 bytes, got \{raw.length()}")
  }
  if raw[0].to_int() != 0xFF || raw[9].to_int() != 0x7F {
    raise ProtocolError("invalid ZMTP greeting signature")
  }
  let version_major = raw[10].to_int()
  let version_minor = raw[11].to_int()
  if version_major < 3 {
    raise ProtocolError(
      "unsupported ZMTP major version: \{version_major}.\{version_minor}",
    )
  }
  let sb = StringBuilder()
  let mut seen_null = false
  for i = 12; i < 32; i = i + 1 {
    let b = raw[i].to_int()
    if b == 0 {
      seen_null = true
    } else if seen_null {
      raise ProtocolError("non-null byte after null padding in mechanism field")
    } else if b < 0x20 || b > 0x7E {
      raise ProtocolError("invalid character in mechanism field: \{b}")
    } else {
      sb.write_char(Int::unsafe_to_char(b))
    }
  }
  let mechanism = sb.to_string()
  if mechanism.is_empty() {
    raise ProtocolError("empty security mechanism in greeting")
  }
  let server_flag = raw[32].to_int()
  if server_flag != 0 && server_flag != 1 {
    raise ProtocolError("invalid as-server flag in greeting: \{server_flag}")
  }
  {
    version_major,
    version_minor,
    mechanism,
    as_server: server_flag == 1,
  }
}

///|
/// ZMTP 3.1 handshake metadata properties (`Socket-Type`, `Identity`, and custom keys).
pub struct Metadata {
  socket_type : String
  identity : Bytes?
  properties : Array[(String, Bytes)]
} derive(Eq, Debug)

///|
pub extend Metadata with Eq::{not_equal, equal}

///|
pub extend Metadata with @debug.Debug::{to_repr}

///|
/// Construct handshake metadata.
pub fn Metadata::new(
  socket_type : String,
  identity? : Bytes? = None,
  properties? : Array[(String, Bytes)] = [],
) -> Metadata {
  { socket_type, identity, properties }
}

///|
fn append_metadata_property(
  out : Array[Byte],
  name : String,
  value : Bytes,
) -> Unit {
  let nlen = name.length() & 0xFF
  out.push(nlen.to_byte())
  for i = 0; i < nlen; i = i + 1 {
    out.push((name.code_unit_at(i).to_int() & 0xFF).to_byte())
  }
  let vlen = value.length()
  out.push(((vlen >> 24) & 0xFF).to_byte())
  out.push(((vlen >> 16) & 0xFF).to_byte())
  out.push(((vlen >> 8) & 0xFF).to_byte())
  out.push((vlen & 0xFF).to_byte())
  for i = 0; i < vlen; i = i + 1 {
    out.push(value[i])
  }
}

///|
/// Serialize metadata into ZMTP property key-value wire format.
pub fn Metadata::encode(self : Metadata) -> Bytes {
  let out : Array[Byte] = []
  append_metadata_property(
    out,
    "Socket-Type",
    string_to_ascii_bytes(self.socket_type),
  )
  match self.identity {
    Some(id) =>
      if id.length() > 0 {
        append_metadata_property(out, "Identity", id)
      }
    None => ()
  }
  for pair in self.properties {
    let (k, v) = pair
    append_metadata_property(out, k, v)
  }
  Bytes::from_array(out)
}

///|
/// Parse ZMTP metadata wire bytes into `Metadata`.
/// Property names are matched case-insensitively per RFC 23 Section 3.3.
pub fn Metadata::decode(raw : Bytes) -> Metadata raise ZmqError {
  let mut pos = 0
  let n = raw.length()
  let mut socket_type = ""
  let mut identity : Bytes? = None
  let properties : Array[(String, Bytes)] = []
  while pos < n {
    let klen = raw[pos].to_int()
    pos = pos + 1
    if klen == 0 || pos + klen + 4 > n {
      raise ProtocolError("malformed metadata property header")
    }
    let key = ascii_bytes_to_string(slice_bytes(raw, pos, pos + klen))
    pos = pos + klen
    let vlen = (raw[pos].to_int() << 24) |
      (raw[pos + 1].to_int() << 16) |
      (raw[pos + 2].to_int() << 8) |
      raw[pos + 3].to_int()
    pos = pos + 4
    if vlen < 0 || pos + vlen > n {
      raise ProtocolError("malformed metadata property value length: \{vlen}")
    }
    let val = slice_bytes(raw, pos, pos + vlen)
    pos = pos + vlen
    let upper_key = ascii_to_upper(key)
    if upper_key == "SOCKET-TYPE" {
      socket_type = ascii_bytes_to_string(val)
    } else if upper_key == "IDENTITY" {
      if val.length() > 255 {
        raise ProtocolError("Identity metadata exceeds 255 bytes")
      }
      identity = Some(val)
    } else {
      properties.push((key, val))
    }
  }
  if socket_type.is_empty() {
    raise ProtocolError("missing required Socket-Type in metadata")
  }
  { socket_type, identity, properties }
}

///|
/// Look up a metadata property value by case-insensitive key name.
pub fn Metadata::get(self : Metadata, key : String) -> Bytes? {
  let target = ascii_to_upper(key)
  if target == "SOCKET-TYPE" {
    return Some(string_to_ascii_bytes(self.socket_type))
  }
  if target == "IDENTITY" {
    return self.identity
  }
  for pair in self.properties {
    let (k, v) = pair
    if ascii_to_upper(k) == target {
      return Some(v)
    }
  }
  None
}

///|
/// ZMTP 3.1 and PLAIN (RFC 24) command frames.
pub(all) enum ZmtpCommand {
  Ready(Metadata)
  ErrorCmd(String)
  Ping(Int, Bytes)
  Pong(Bytes)
  Subscribe(Bytes)
  Cancel(Bytes)
  Hello(String, String)
  Welcome
  Initiate(Metadata)
  Unknown(String, Bytes)
} derive(Eq, Debug)

///|
pub extend ZmtpCommand with Eq::{not_equal, equal}

///|
pub extend ZmtpCommand with @debug.Debug::{to_repr}

///|
/// Encode a `ZmtpCommand` into a complete ZMTP command wire frame.
pub fn ZmtpCommand::encode_frame(self : ZmtpCommand) -> Bytes {
  let body : Array[Byte] = []
  match self {
    Ready(meta) => {
      body.push(b'\x05')
      for b in b"READY" {
        body.push(b)
      }
      for b in meta.encode() {
        body.push(b)
      }
    }
    ErrorCmd(reason) => {
      body.push(b'\x05')
      for b in b"ERROR" {
        body.push(b)
      }
      let rbytes = string_to_ascii_bytes(reason)
      let rlen = if rbytes.length() > 255 { 255 } else { rbytes.length() }
      body.push(rlen.to_byte())
      for i = 0; i < rlen; i = i + 1 {
        body.push(rbytes[i])
      }
    }
    Ping(ttl, ctx) => {
      body.push(b'\x04')
      for b in b"PING" {
        body.push(b)
      }
      body.push(((ttl >> 8) & 0xFF).to_byte())
      body.push((ttl & 0xFF).to_byte())
      let clen = if ctx.length() > 16 { 16 } else { ctx.length() }
      for i = 0; i < clen; i = i + 1 {
        body.push(ctx[i])
      }
    }
    Pong(ctx) => {
      body.push(b'\x04')
      for b in b"PONG" {
        body.push(b)
      }
      let clen = if ctx.length() > 16 { 16 } else { ctx.length() }
      for i = 0; i < clen; i = i + 1 {
        body.push(ctx[i])
      }
    }
    Subscribe(topic) => {
      body.push(b'\x09')
      for b in b"SUBSCRIBE" {
        body.push(b)
      }
      for b in topic {
        body.push(b)
      }
    }
    Cancel(topic) => {
      body.push(b'\x06')
      for b in b"CANCEL" {
        body.push(b)
      }
      for b in topic {
        body.push(b)
      }
    }
    Hello(username, password) => {
      body.push(b'\x05')
      for b in b"HELLO" {
        body.push(b)
      }
      let ubytes = string_to_ascii_bytes(username)
      let ulen = if ubytes.length() > 255 { 255 } else { ubytes.length() }
      body.push(ulen.to_byte())
      for i = 0; i < ulen; i = i + 1 {
        body.push(ubytes[i])
      }
      let pbytes = string_to_ascii_bytes(password)
      let plen = if pbytes.length() > 255 { 255 } else { pbytes.length() }
      body.push(plen.to_byte())
      for i = 0; i < plen; i = i + 1 {
        body.push(pbytes[i])
      }
    }
    Welcome => {
      body.push(b'\x07')
      for b in b"WELCOME" {
        body.push(b)
      }
    }
    Initiate(meta) => {
      body.push(b'\x08')
      for b in b"INITIATE" {
        body.push(b)
      }
      for b in meta.encode() {
        body.push(b)
      }
    }
    Unknown(name, payload) => {
      let nbytes = string_to_ascii_bytes(name)
      body.push((nbytes.length() & 0xFF).to_byte())
      for b in nbytes {
        body.push(b)
      }
      for b in payload {
        body.push(b)
      }
    }
  }
  Frame::command_frame(Bytes::from_array(body)).encode()
}

///|
/// Decode a command frame payload into a typed `ZmtpCommand`.
pub fn ZmtpCommand::decode_body(body : Bytes) -> ZmtpCommand raise ZmqError {
  if body.length() < 1 {
    raise ProtocolError("empty command frame body")
  }
  let name_len = body[0].to_int()
  if name_len == 0 || 1 + name_len > body.length() {
    raise ProtocolError("invalid command name length: \{name_len}")
  }
  let name = ascii_bytes_to_string(slice_bytes(body, 1, 1 + name_len))
  let payload = slice_bytes(body, 1 + name_len, body.length())
  match name {
    "READY" => Ready(Metadata::decode(payload))
    "ERROR" => {
      if payload.length() < 1 {
        raise ProtocolError("ERROR command missing reason length octet")
      }
      let rlen = payload[0].to_int()
      if 1 + rlen > payload.length() {
        raise ProtocolError("ERROR command reason length exceeds payload")
      }
      let reason = ascii_bytes_to_string(slice_bytes(payload, 1, 1 + rlen))
      ErrorCmd(reason)
    }
    "PING" => {
      if payload.length() < 2 {
        raise ProtocolError("PING command requires 2-byte TTL")
      }
      let ttl = (payload[0].to_int() << 8) | payload[1].to_int()
      let ctx = slice_bytes(payload, 2, payload.length())
      if ctx.length() > 16 {
        raise ProtocolError("PING context exceeds 16 bytes")
      }
      Ping(ttl, ctx)
    }
    "PONG" => {
      if payload.length() > 16 {
        raise ProtocolError("PONG context exceeds 16 bytes")
      }
      Pong(payload)
    }
    "SUBSCRIBE" => Subscribe(payload)
    "CANCEL" => Cancel(payload)
    "HELLO" => {
      if payload.length() < 2 {
        raise ProtocolError("HELLO command too short")
      }
      let ulen = payload[0].to_int()
      if 1 + ulen + 1 > payload.length() {
        raise ProtocolError("HELLO username length out of bounds")
      }
      let username = ascii_bytes_to_string(slice_bytes(payload, 1, 1 + ulen))
      let plen = payload[1 + ulen].to_int()
      if 1 + ulen + 1 + plen != payload.length() {
        raise ProtocolError("HELLO password length mismatch")
      }
      let password = ascii_bytes_to_string(
        slice_bytes(payload, 2 + ulen, 2 + ulen + plen),
      )
      Hello(username, password)
    }
    "WELCOME" => {
      if payload.length() != 0 {
        raise ProtocolError("WELCOME command must have empty payload")
      }
      Welcome
    }
    "INITIATE" => Initiate(Metadata::decode(payload))
    _ => Unknown(name, payload)
  }
}

///|
/// Encode a ZMTP 3.0 compatible subscription data message (`\x01` + topic or `\x00` + topic).
pub fn encode_subscription_frame(subscribe : Bool, topic : Bytes) -> Message {
  let out : Array[Byte] = Array(capacity=1 + topic.length())
  out.push(if subscribe { b'\x01' } else { b'\x00' })
  for i = 0; i < topic.length(); i = i + 1 {
    out.push(topic[i])
  }
  Message::from_bytes(Bytes::from_array(out))
}

///|
/// Parse a ZMTP 3.0 subscription data frame (`\x01topic` or `\x00topic`).
/// Returns `Some((true, topic))` for subscribe, `Some((false, topic))` for cancel,
/// or `None` if the message is not a subscription frame.
pub fn decode_subscription_frame(msg : Message) -> (Bool, Bytes)? {
  if msg.frames.length() != 1 {
    return None
  }
  let f = msg.frames[0]
  if f.length() < 1 {
    return None
  }
  let tag = f[0].to_int()
  if tag == 1 {
    Some((true, slice_bytes(f, 1, f.length())))
  } else if tag == 0 {
    Some((false, slice_bytes(f, 1, f.length())))
  } else {
    None
  }
}

///|
/// High-level events produced by a ZMTP 3.1 peer session state machine.
pub(all) enum SessionEvent {
  HandshakeReady(Metadata)
  MessageReceived(Message)
  CommandReceived(ZmtpCommand)
  SendBytes(Bytes)
  ProtocolFailure(String)
} derive(Eq, Debug)

///|
pub extend SessionEvent with Eq::{not_equal, equal}

///|
pub extend SessionEvent with @debug.Debug::{to_repr}

///|
enum NullSessionState {
  AwaitGreeting
  AwaitReady
  Established
  Failed
} derive(Eq)

///|
pub extend NullSessionState with Eq::{not_equal, equal}

///|
/// Streaming ZMTP 3.1 session state machine for the `NULL` security mechanism.
/// Handles arbitrary byte chunking across greeting, READY handshake, PING/PONG,
/// ERROR frames, and multipart data messages.
pub struct NullSession {
  local_type : SocketType
  local_identity : Bytes?
  mut state : NullSessionState
  mut greet_buf : Array[Byte]
  decoder : FrameDecoder
  mut peer_metadata : Metadata?
}

///|
/// Create a new `NullSession` for the given local socket type and optional identity.
pub fn NullSession::new(
  local_type : SocketType,
  local_identity? : Bytes? = None,
  max_frame_size? : Int = DEFAULT_MAX_FRAME_SIZE,
) -> NullSession {
  {
    local_type,
    local_identity,
    state: AwaitGreeting,
    greet_buf: [],
    decoder: FrameDecoder::new(max_frame_size~),
    peer_metadata: None,
  }
}

///|
/// Produce the initial outbound greeting bytes for a `NULL` session.
pub fn NullSession::initial_greeting(_self : NullSession) -> Bytes {
  Greeting::new(mechanism="NULL", as_server=false).encode()
}

///|
/// Produce the outbound `READY` command frame for this session.
pub fn NullSession::local_ready_frame(self : NullSession) -> Bytes {
  let meta = Metadata::new(
    self.local_type.to_zmtp_name(),
    identity=self.local_identity,
  )
  ZmtpCommand::Ready(meta).encode_frame()
}

///|
/// Return the negotiated peer metadata once the handshake is complete.
pub fn NullSession::peer_metadata(self : NullSession) -> Metadata? {
  self.peer_metadata
}

///|
/// Feed raw wire bytes into the `NullSession` and return resulting session events.
pub fn NullSession::feed(
  self : NullSession,
  chunk : Bytes,
) -> Array[SessionEvent] {
  if self.state == Failed {
    return []
  }
  let events : Array[SessionEvent] = []
  let mut offset = 0
  if self.state == AwaitGreeting {
    let needed = 64 - self.greet_buf.length()
    let take = if chunk.length() < needed { chunk.length() } else { needed }
    for i = 0; i < take; i = i + 1 {
      self.greet_buf.push(chunk[i])
    }
    offset = take
    if self.greet_buf.length() < 64 {
      return events
    }
    let greet_bytes = Bytes::from_array(self.greet_buf)
    self.greet_buf = []
    let greet = Greeting::decode(greet_bytes) catch {
      err => {
        self.state = Failed
        let reason = match err {
          ProtocolError(msg) => msg
          _ => "invalid greeting"
        }
        events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
        events.push(ProtocolFailure(reason))
        return events
      }
    }
    if greet.mechanism != "NULL" {
      self.state = Failed
      let reason = "expected NULL mechanism, got \{greet.mechanism}"
      events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
      events.push(ProtocolFailure(reason))
      return events
    }
    self.state = AwaitReady
    events.push(SendBytes(self.local_ready_frame()))
  }
  let rest = if offset == 0 {
    chunk
  } else {
    slice_bytes(chunk, offset, chunk.length())
  }
  if rest.length() == 0 && self.state != AwaitReady {
    return events
  }
  let items = self.decoder.feed(rest) catch {
    err => {
      self.state = Failed
      let reason = match err {
        ProtocolError(msg) => msg
        FrameTooLarge(sz) => "frame size \{sz} exceeds limit"
        _ => "frame decode error"
      }
      events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
      events.push(ProtocolFailure(reason))
      return events
    }
  }
  for item in items {
    if self.state == Failed {
      break
    }
    match (self.state, item) {
      (AwaitReady, CommandFrame(body)) => {
        let cmd = ZmtpCommand::decode_body(body) catch {
          err => {
            self.state = Failed
            let reason = match err {
              ProtocolError(msg) => msg
              _ => "invalid handshake command"
            }
            events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
            events.push(ProtocolFailure(reason))
            break
          }
        }
        match cmd {
          Ready(meta) =>
            match SocketType::from_zmtp_name(meta.socket_type) {
              Some(peer_st) =>
                if self.local_type.is_compatible_with(peer_st) {
                  self.peer_metadata = Some(meta)
                  self.state = Established
                  events.push(HandshakeReady(meta))
                } else {
                  self.state = Failed
                  let reason = "incompatible socket types: local \{self.local_type.to_zmtp_name()} vs peer \{meta.socket_type}"
                  events.push(
                    SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()),
                  )
                  events.push(ProtocolFailure(reason))
                }
              None => {
                self.state = Failed
                let reason = "unknown peer Socket-Type: \{meta.socket_type}"
                events.push(
                  SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()),
                )
                events.push(ProtocolFailure(reason))
              }
            }
          ErrorCmd(reason) => {
            self.state = Failed
            events.push(ProtocolFailure("peer sent ERROR: \{reason}"))
          }
          _ => {
            self.state = Failed
            let reason = "expected READY command during NULL handshake"
            events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
            events.push(ProtocolFailure(reason))
          }
        }
      }
      (AwaitReady, DataMessage(_)) => {
        self.state = Failed
        let reason = "received data frame before READY handshake completed"
        events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
        events.push(ProtocolFailure(reason))
      }
      (Established, DataMessage(msg)) => events.push(MessageReceived(msg))
      (Established, CommandFrame(body)) => {
        let cmd = ZmtpCommand::decode_body(body) catch {
          err => {
            self.state = Failed
            let reason = match err {
              ProtocolError(msg) => msg
              _ => "malformed command frame"
            }
            events.push(SendBytes(ZmtpCommand::ErrorCmd(reason).encode_frame()))
            events.push(ProtocolFailure(reason))
            break
          }
        }
        match cmd {
          Ping(_ttl, ctx) => {
            events.push(SendBytes(ZmtpCommand::Pong(ctx).encode_frame()))
            events.push(CommandReceived(cmd))
          }
          ErrorCmd(reason) => {
            self.state = Failed
            events.push(ProtocolFailure("peer sent ERROR: \{reason}"))
          }
          _ => events.push(CommandReceived(cmd))
        }
      }
      _ => ()
    }
  }
  events
}
