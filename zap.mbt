///|
/// ZeroMQ RFC 27 ZAP 1.0 (ZeroMQ Authentication Protocol) wire codec,
/// IPv4/CIDR access control lists, domain-scoped credential store, audit log,
/// and `inproc://zeromq.zap.01` asynchronous authentication responder.

///|
/// Canonical ZAP protocol version string defined by ZeroMQ RFC 27.
pub const ZAP_VERSION : String = "1.0"

///|
/// Standard in-process endpoint URI used by ZeroMQ ZAP authenticators (RFC 27).
pub const ZAP_DEFAULT_ENDPOINT : String = "inproc://zeromq.zap.01"

///|
/// Status codes returned in frame 2 of a ZAP 1.0 reply (RFC 27 Section 2.2).
pub(all) enum ZapStatusCode {
  /// `"200"` — Authentication succeeded.
  Ok200
  /// `"300"` — Temporary failure (client may retry later).
  TemporaryError300
  /// `"400"` — Authentication rejected due to invalid credentials or IP policy.
  Rejected400
  /// `"500"` — Internal error inside the ZAP handler.
  InternalError500
} derive(Eq, Debug)

///|
pub extend ZapStatusCode with Eq::{not_equal, equal}

///|
pub extend ZapStatusCode with @debug.Debug::{to_repr}

///|
/// Convert a `ZapStatusCode` to its canonical 3-character RFC 27 status string.
pub fn ZapStatusCode::to_code_string(self : ZapStatusCode) -> String {
  match self {
    Ok200 => "200"
    TemporaryError300 => "300"
    Rejected400 => "400"
    InternalError500 => "500"
  }
}

///|
/// Parse a 3-character RFC 27 status string into `ZapStatusCode`.
pub fn ZapStatusCode::from_code_string(code : String) -> ZapStatusCode? {
  match code {
    "200" => Some(Ok200)
    "300" => Some(TemporaryError300)
    "400" => Some(Rejected400)
    "500" => Some(InternalError500)
    _ => None
  }
}

///|
/// RFC 27 ZAP 1.0 authentication request message structure.
pub struct ZapRequest {
  version : String
  request_id : Bytes
  domain : String
  address : String
  identity : Bytes
  mechanism : String
  credentials : Array[Bytes]
} derive(Eq, Debug)

///|
pub extend ZapRequest with Eq::{not_equal, equal}

///|
pub extend ZapRequest with @debug.Debug::{to_repr}

///|
/// Construct a `ZapRequest` for the `NULL` mechanism (IP/domain check only).
pub fn ZapRequest::new_null(
  request_id : Bytes,
  address : String,
  domain~ : String = "global",
  identity~ : Bytes = b"",
) -> ZapRequest {
  {
    version: ZAP_VERSION,
    request_id,
    domain,
    address,
    identity,
    mechanism: "NULL",
    credentials: [],
  }
}

///|
/// Construct a `ZapRequest` for the `PLAIN` mechanism carrying `[username, password]`.
pub fn ZapRequest::new_plain(
  request_id : Bytes,
  address : String,
  username : String,
  password : String,
  domain~ : String = "global",
  identity~ : Bytes = b"",
) -> ZapRequest {
  {
    version: ZAP_VERSION,
    request_id,
    domain,
    address,
    identity,
    mechanism: "PLAIN",
    credentials: [
      string_to_ascii_bytes(username),
      string_to_ascii_bytes(password),
    ],
  }
}

///|
/// Construct a `ZapRequest` for the `CURVE` mechanism carrying a 32-byte client public key.
pub fn ZapRequest::new_curve(
  request_id : Bytes,
  address : String,
  client_public_key : Bytes,
  domain~ : String = "global",
  identity~ : Bytes = b"",
) -> ZapRequest {
  {
    version: ZAP_VERSION,
    request_id,
    domain,
    address,
    identity,
    mechanism: "CURVE",
    credentials: [client_public_key],
  }
}

///|
/// Serialize a `ZapRequest` into an RFC 27 multipart `Message` (`6 + N` frames).
pub fn ZapRequest::to_message(self : ZapRequest) -> Message {
  let frames : Array[Bytes] = Array(capacity=6 + self.credentials.length())
  frames.push(string_to_ascii_bytes(self.version))
  frames.push(self.request_id)
  frames.push(string_to_ascii_bytes(self.domain))
  frames.push(string_to_ascii_bytes(self.address))
  frames.push(self.identity)
  frames.push(string_to_ascii_bytes(self.mechanism))
  for cred in self.credentials {
    frames.push(cred)
  }
  Message::multipart(frames)
}

///|
/// Parse an RFC 27 multipart `Message` into a `ZapRequest`.
pub fn ZapRequest::from_message(msg : Message) -> ZapRequest raise ZmqError {
  if msg.frames.length() < 6 {
    let n = msg.frames.length()
    raise ProtocolError(
      "ZAP request requires at least 6 frames per RFC 27, got \{n}",
    )
  }
  let version = ascii_bytes_to_string(msg.frames[0])
  if version != ZAP_VERSION {
    raise ProtocolError("unsupported ZAP request version: \{version}")
  }
  let request_id = msg.frames[1]
  let domain = ascii_bytes_to_string(msg.frames[2])
  let address = ascii_bytes_to_string(msg.frames[3])
  let identity = msg.frames[4]
  let mechanism = ascii_bytes_to_string(msg.frames[5])
  let credentials : Array[Bytes] = Array(capacity=msg.frames.length() - 6)
  for i = 6; i < msg.frames.length(); i = i + 1 {
    credentials.push(msg.frames[i])
  }
  { version, request_id, domain, address, identity, mechanism, credentials, }
}

///|
/// Encode a list of `(key, value)` properties into an RFC 23/27 ZAP metadata blob
/// (`[1-byte key_len][key][4-byte big-endian val_len][val]...`).
pub fn encode_zap_metadata_blob(
  properties : Array[(String, String)],
) -> Bytes raise ZmqError {
  let w = WireWriter::new(capacity=64)
  for pair in properties {
    let (k, v) = pair
    if k.is_empty() || k.length() > 255 {
      raise ProtocolError("ZAP metadata key length must be 1..255 bytes")
    }
    w.write_short_string(k)
    w.write_long_string(v)
  }
  w.to_bytes()
}

///|
/// Decode an RFC 23/27 binary metadata blob from a `ZapReply` into `(key, value)` string pairs.
pub fn decode_zap_metadata_blob(
  blob : Bytes,
) -> Array[(String, String)] raise ZmqError {
  let r = WireReader::new(blob)
  let out : Array[(String, String)] = []
  while !r.is_eof() {
    let k = r.read_short_string()
    if k.is_empty() {
      raise ProtocolError("ZAP metadata property key cannot be empty")
    }
    let v = r.read_long_string()
    out.push((k, v))
  }
  out
}

///|
/// RFC 27 ZAP 1.0 authentication reply message structure (6 frames).
pub struct ZapReply {
  version : String
  request_id : Bytes
  status_code : ZapStatusCode
  status_text : String
  user_id : String
  metadata : Bytes
} derive(Eq, Debug)

///|
pub extend ZapReply with Eq::{not_equal, equal}

///|
pub extend ZapReply with @debug.Debug::{to_repr}

///|
/// Construct a `200 OK` `ZapReply`.
pub fn ZapReply::ok(
  request_id : Bytes,
  user_id~ : String = "",
  status_text~ : String = "OK",
  metadata~ : Bytes = b"",
) -> ZapReply {
  {
    version: ZAP_VERSION,
    request_id,
    status_code: Ok200,
    status_text,
    user_id,
    metadata,
  }
}

///|
/// Construct a non-200 failure `ZapReply`.
pub fn ZapReply::reject(
  request_id : Bytes,
  status_code~ : ZapStatusCode = Rejected400,
  status_text~ : String = "Unauthorized",
) -> ZapReply {
  {
    version: ZAP_VERSION,
    request_id,
    status_code,
    status_text,
    user_id: "",
    metadata: b"",
  }
}

///|
/// Whether this `ZapReply` indicates successful authentication (`status_code == Ok200`).
pub fn ZapReply::is_authenticated(self : ZapReply) -> Bool {
  self.status_code == Ok200
}

///|
/// Serialize a `ZapReply` into an exact 6-frame RFC 27 multipart `Message`.
pub fn ZapReply::to_message(self : ZapReply) -> Message {
  Message::multipart([
    string_to_ascii_bytes(self.version),
    self.request_id,
    string_to_ascii_bytes(self.status_code.to_code_string()),
    string_to_ascii_bytes(self.status_text),
    string_to_ascii_bytes(self.user_id),
    self.metadata,
  ])
}

///|
/// Parse a 6-frame RFC 27 multipart `Message` into a `ZapReply`.
pub fn ZapReply::from_message(msg : Message) -> ZapReply raise ZmqError {
  if msg.frames.length() != 6 {
    let n = msg.frames.length()
    raise ProtocolError("ZAP reply must contain exactly 6 frames, got \{n}")
  }
  let version = ascii_bytes_to_string(msg.frames[0])
  if version != ZAP_VERSION {
    raise ProtocolError("unsupported ZAP reply version: \{version}")
  }
  let request_id = msg.frames[1]
  let code_str = ascii_bytes_to_string(msg.frames[2])
  let status_code = match ZapStatusCode::from_code_string(code_str) {
    Some(c) => c
    None => raise ProtocolError("invalid ZAP status code: \{code_str}")
  }
  let status_text = ascii_bytes_to_string(msg.frames[3])
  let user_id = ascii_bytes_to_string(msg.frames[4])
  let metadata = msg.frames[5]
  { version, request_id, status_code, status_text, user_id, metadata, }
}

///|
/// Parsed IPv4 CIDR network rule (e.g. `"127.0.0.1/32"` or `"10.0.0.0/8"`).
pub struct Ipv4Cidr {
  network : Int64
  mask : Int64
  prefix_len : Int32
  text : String
} derive(Eq, Debug)

///|
pub extend Ipv4Cidr with Eq::{not_equal, equal}

///|
pub extend Ipv4Cidr with @debug.Debug::{to_repr}

///|
/// Parse a dotted-quad IPv4 address (e.g. `"192.168.1.10"`) into an unsigned 32-bit value
/// stored in `Int64`. Strips optional `:port` suffix if present.
pub fn parse_ipv4_u32(addr_str : String) -> Int64 raise ZmqError {
  let mut colon_pos = -1
  for i = 0; i < addr_str.length(); i = i + 1 {
    if addr_str.code_unit_at(i).to_int() == 58 {
      colon_pos = i
      break
    }
  }
  let host = if colon_pos >= 0 {
    addr_str[:colon_pos].to_owned()
  } else {
    addr_str
  }
  let octets : Array[Int32] = []
  let mut start = 0
  for i = 0; i <= host.length(); i = i + 1 {
    if i == host.length() || host.code_unit_at(i).to_int() == 46 {
      let part = host[start:i].to_owned()
      if part.is_empty() {
        raise InvalidEndpoint("empty octet in IPv4 address: \{addr_str}")
      }
      let v = @string.parse_int(part) catch {
        _ => raise InvalidEndpoint("invalid octet in IPv4 address: \{addr_str}")
      }
      if v < 0 || v > 255 {
        raise InvalidEndpoint("IPv4 octet out of range 0..255: \{addr_str}")
      }
      octets.push(v)
      start = i + 1
    }
  }
  if octets.length() != 4 {
    raise InvalidEndpoint("expected 4 octets in IPv4 address: \{addr_str}")
  }
  let b0 = octets[0].to_int64()
  let b1 = octets[1].to_int64()
  let b2 = octets[2].to_int64()
  let b3 = octets[3].to_int64()
  (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
}

///|
/// Parse an IPv4 address or CIDR block (`"10.20.0.0/16"` or `"127.0.0.1"`).
pub fn Ipv4Cidr::parse(cidr : String) -> Ipv4Cidr raise ZmqError {
  let mut slash_pos = -1
  for i = 0; i < cidr.length(); i = i + 1 {
    if cidr.code_unit_at(i).to_int() == 47 {
      slash_pos = i
      break
    }
  }
  let (ip_part, prefix_len) = if slash_pos >= 0 {
    let ip_s = cidr[:slash_pos].to_owned()
    let p_s = cidr[slash_pos + 1:].to_owned()
    let plen = @string.parse_int(p_s) catch {
      _ => raise InvalidEndpoint("invalid CIDR prefix length in: \{cidr}")
    }
    if plen < 0 || plen > 32 {
      raise InvalidEndpoint("CIDR prefix length out of range 0..32: \{cidr}")
    }
    (ip_s, plen)
  } else {
    (cidr, 32)
  }
  let raw_ip = parse_ipv4_u32(ip_part)
  let mask = if prefix_len == 0 {
    0L
  } else if prefix_len == 32 {
    0xFFFFFFFFL
  } else {
    let host_bits = 32 - prefix_len
    (0xFFFFFFFFL >> host_bits << host_bits) & 0xFFFFFFFFL
  }
  { network: raw_ip & mask, mask, prefix_len, text: cidr, }
}

///|
/// Test whether `ip_str` falls within this `Ipv4Cidr` subnet.
pub fn Ipv4Cidr::contains(self : Ipv4Cidr, ip_str : String) -> Bool {
  let ip_val = parse_ipv4_u32(ip_str) catch { _ => return false }
  (ip_val & self.mask) == self.network
}

///|
/// User account record inside a `ZapPolicyEngine` domain credential store.
priv struct ZapUserRecord {
  domain : String
  username : String
  password_bytes : Bytes
  properties : Array[(String, String)]
}

///|
/// Structured audit log record emitted for each evaluated ZAP request.
pub struct ZapAuditEntry {
  timestamp_ms : Int64
  domain : String
  address : String
  mechanism : String
  principal : String
  status_code : ZapStatusCode
  reason : String
} derive(Eq, Debug)

///|
pub extend ZapAuditEntry with Eq::{not_equal, equal}

///|
pub extend ZapAuditEntry with @debug.Debug::{to_repr}

///|
/// Configurable RFC 27 ZAP policy engine supporting IPv4 CIDR allowlists/blocklists,
/// domain-scoped `PLAIN` user accounts with custom ZMTP metadata injection, `CURVE`
/// Z85 public-key whitelists, and a bounded authentication audit log.
pub struct ZapPolicyEngine {
  allowlist : Array[Ipv4Cidr]
  blocklist : Array[Ipv4Cidr]
  priv users : Array[ZapUserRecord]
  priv curve_keys : Array[(String, Bytes)]
  mut curve_allow_any : Bool
  max_audit_entries : Int32
  audit_log : Array[ZapAuditEntry]
}

///|
/// Create a new `ZapPolicyEngine` with optional `max_audit_entries` ring capacity.
pub fn ZapPolicyEngine::new(
  max_audit_entries~ : Int32 = 256,
) -> ZapPolicyEngine {
  let cap = if max_audit_entries < 0 { 0 } else { max_audit_entries }
  {
    allowlist: [],
    blocklist: [],
    users: [],
    curve_keys: [],
    curve_allow_any: false,
    max_audit_entries: cap,
    audit_log: [],
  }
}

///|
/// Add an IPv4 address or CIDR subnet to the allowlist (matched before credentials).
pub fn ZapPolicyEngine::allow_ip(
  self : ZapPolicyEngine,
  cidr : String,
) -> Unit raise ZmqError {
  self.allowlist.push(Ipv4Cidr::parse(cidr))
}

///|
/// Add an IPv4 address or CIDR subnet to the blocklist.
pub fn ZapPolicyEngine::block_ip(
  self : ZapPolicyEngine,
  cidr : String,
) -> Unit raise ZmqError {
  self.blocklist.push(Ipv4Cidr::parse(cidr))
}

///|
/// Register or update a `PLAIN` user credential in `domain` (`"*"` matches all domains),
/// optionally attaching custom ZMTP metadata properties returned on `200 OK`.
pub fn ZapPolicyEngine::upsert_plain_user(
  self : ZapPolicyEngine,
  username : String,
  password : String,
  domain~ : String = "*",
  properties~ : Array[(String, String)] = [],
) -> Unit {
  let pw_bytes = string_to_ascii_bytes(password)
  for i = 0; i < self.users.length(); i = i + 1 {
    if self.users[i].domain == domain && self.users[i].username == username {
      self.users[i] = {
        domain,
        username,
        password_bytes: pw_bytes,
        properties,
      }
      return
    }
  }
  self.users.push({ domain, username, password_bytes: pw_bytes, properties, })
}

///|
/// Remove a `PLAIN` user from `domain`. Returns `true` if removed.
pub fn ZapPolicyEngine::remove_plain_user(
  self : ZapPolicyEngine,
  username : String,
  domain~ : String = "*",
) -> Bool {
  for i = 0; i < self.users.length(); i = i + 1 {
    if self.users[i].domain == domain && self.users[i].username == username {
      let _ = self.users.remove(i)
      return true
    }
  }
  false
}

///|
/// Authorize a 40-character Z85-encoded `CURVE` 32-byte client public key and bind it
/// to a principal `label`.
pub fn ZapPolicyEngine::authorize_curve_z85(
  self : ZapPolicyEngine,
  label : String,
  z85_public_key : String,
) -> Unit raise ZmqError {
  let raw_key = z85_decode(z85_public_key)
  if raw_key.length() != 32 {
    let n = raw_key.length()
    raise ProtocolError("CURVE public key must decode to 32 bytes, got \{n}")
  }
  self.curve_keys.push((label, raw_key))
}

///|
/// Configure whether any syntactically valid 32-byte `CURVE` key is permitted (`CURVE_ALLOW_ANY`).
pub fn ZapPolicyEngine::set_curve_allow_any(
  self : ZapPolicyEngine,
  allow_any : Bool,
) -> Unit {
  self.curve_allow_any = allow_any
}

///|
fn ZapPolicyEngine::record_audit(
  self : ZapPolicyEngine,
  now_ms : Int64,
  req : ZapRequest,
  principal : String,
  status_code : ZapStatusCode,
  reason : String,
) -> Unit {
  if self.max_audit_entries <= 0 {
    return
  }
  if self.audit_log.length() >= self.max_audit_entries {
    let _ = self.audit_log.remove(0)
  }
  self.audit_log.push({
    timestamp_ms: now_ms,
    domain: req.domain,
    address: req.address,
    mechanism: req.mechanism,
    principal,
    status_code,
    reason,
  })
}

///|
fn ZapPolicyEngine::check_ip_allowed(
  self : ZapPolicyEngine,
  address : String,
) -> Result[Unit, String] {
  if address.is_empty() {
    if !self.allowlist.is_empty() {
      return Err("empty client IP rejected by active allowlist")
    }
    return Ok(())
  }
  for rule in self.blocklist {
    if rule.contains(address) {
      return Err("IP \{address} blocked by rule \{rule.text}")
    }
  }
  if !self.allowlist.is_empty() {
    let mut matched = false
    for rule in self.allowlist {
      if rule.contains(address) {
        matched = true
        break
      }
    }
    if !matched {
      return Err("IP \{address} not in allowlist")
    }
  }
  Ok(())
}

///|
/// Evaluate a `ZapRequest` against IP policies and mechanism credentials, returning
/// an RFC 27 `ZapReply` and appending an entry to `audit_log`.
pub fn ZapPolicyEngine::evaluate(
  self : ZapPolicyEngine,
  req : ZapRequest,
  now_ms~ : Int64 = 0L,
) -> ZapReply {
  match self.check_ip_allowed(req.address) {
    Err(ip_reason) => {
      self.record_audit(now_ms, req, "", Rejected400, ip_reason)
      return ZapReply::reject(
        req.request_id,
        status_code=Rejected400,
        status_text=ip_reason,
      )
    }
    Ok(_) => ()
  }
  match req.mechanism {
    "NULL" => {
      let uid = if req.address.is_empty() { "anonymous" } else { req.address }
      self.record_audit(now_ms, req, uid, Ok200, "OK")
      ZapReply::ok(req.request_id, user_id=uid, status_text="OK")
    }
    "PLAIN" => {
      if req.credentials.length() != 2 {
        let reason = "PLAIN requires exactly 2 credential frames"
        self.record_audit(now_ms, req, "", Rejected400, reason)
        return ZapReply::reject(
          req.request_id,
          status_code=Rejected400,
          status_text=reason,
        )
      }
      let uname = ascii_bytes_to_string(req.credentials[0])
      let pw = req.credentials[1]
      for rec in self.users {
        let domain_ok = rec.domain == "*" || rec.domain == req.domain
        if domain_ok &&
          rec.username == uname &&
          constant_time_bytes_eq(rec.password_bytes, pw) {
          let all_props : Array[(String, String)] = [("User-Id", uname)]
          for p in rec.properties {
            all_props.push(p)
          }
          let meta_blob = encode_zap_metadata_blob(all_props) catch { _ => b"" }
          self.record_audit(now_ms, req, uname, Ok200, "OK")
          return ZapReply::ok(
            req.request_id,
            user_id=uname,
            status_text="OK",
            metadata=meta_blob,
          )
        }
      }
      let reason = "invalid PLAIN username or password"
      self.record_audit(now_ms, req, uname, Rejected400, reason)
      ZapReply::reject(
        req.request_id,
        status_code=Rejected400,
        status_text=reason,
      )
    }
    "CURVE" => {
      if req.credentials.length() != 1 || req.credentials[0].length() != 32 {
        let reason = "CURVE requires a single 32-byte public key credential"
        self.record_audit(now_ms, req, "", Rejected400, reason)
        return ZapReply::reject(
          req.request_id,
          status_code=Rejected400,
          status_text=reason,
        )
      }
      let client_key = req.credentials[0]
      if self.curve_allow_any {
        let z85_id = z85_encode(client_key) catch { _ => "curve-client" }
        self.record_audit(now_ms, req, z85_id, Ok200, "CURVE_ALLOW_ANY")
        return ZapReply::ok(req.request_id, user_id=z85_id, status_text="OK")
      }
      for entry in self.curve_keys {
        let (label, authorized_key) = entry
        if constant_time_bytes_eq(authorized_key, client_key) {
          self.record_audit(now_ms, req, label, Ok200, "OK")
          return ZapReply::ok(req.request_id, user_id=label, status_text="OK")
        }
      }
      let reason = "unauthorized CURVE client public key"
      self.record_audit(now_ms, req, "", Rejected400, reason)
      ZapReply::reject(
        req.request_id,
        status_code=Rejected400,
        status_text=reason,
      )
    }
    other => {
      let reason = "unsupported ZAP security mechanism: \{other}"
      self.record_audit(now_ms, req, "", InternalError500, reason)
      ZapReply::reject(
        req.request_id,
        status_code=InternalError500,
        status_text=reason,
      )
    }
  }
}

///|
/// Convert this `ZapPolicyEngine` into a `Socket::set_plain_server` authenticator closure
/// scoped to `domain` and optional peer `address`.
pub fn ZapPolicyEngine::as_plain_authenticator(
  self : ZapPolicyEngine,
  domain~ : String = "global",
  address~ : String = "127.0.0.1",
) -> (String, String) -> Bool {
  fn(username : String, password : String) -> Bool {
    let req = ZapRequest::new_plain(
      b"inline",
      address,
      username,
      password,
      domain~,
    )
    self.evaluate(req).is_authenticated()
  }
}

///|
/// Spawn an asynchronous RFC 27 ZAP `REP` server inside `ctx` bound to `endpoint`
/// (defaults to `"inproc://zeromq.zap.01"`), returning the server `Socket` so the caller
/// or `ctx.term()` can stop it cleanly.
pub async fn ZapPolicyEngine::spawn_inproc_server(
  self : ZapPolicyEngine,
  group : @async.TaskGroup,
  ctx : Context,
  endpoint~ : String = ZAP_DEFAULT_ENDPOINT,
) -> Socket raise ZmqError {
  let rep = ctx.socket(Rep)
  let _ = rep.bind(group, endpoint)
  let _ = group.spawn(no_wait=true, allow_failure=true, () => {
    while !rep.is_closed() && !@async.is_being_cancelled() {
      let raw_msg = rep.recv() catch { _ => break }
      let parsed : Result[ZapRequest, ZmqError] = Ok(
        ZapRequest::from_message(raw_msg),
      ) catch {
        err => Err(err)
      }
      let reply_msg = match parsed {
        Ok(req) => self.evaluate(req, now_ms=@async.now()).to_message()
        Err(_) => {
          let req_id = if raw_msg.frames.length() >= 2 {
            raw_msg.frames[1]
          } else {
            b""
          }
          ZapReply::reject(
            req_id,
            status_code=InternalError500,
            status_text="Malformed ZAP 1.0 request",
          ).to_message()
        }
      }
      rep.send(reply_msg) catch {
        _ => break
      }
    }
  })
  rep
}

///|
/// Client helper sending a `ZapRequest` over a `REQ` socket connected to a ZAP server
/// (such as `"inproc://zeromq.zap.01"`) and decoding the `ZapReply`.
pub async fn zap_authenticate_over_socket(
  req_socket : Socket,
  request : ZapRequest,
) -> ZapReply raise ZmqError {
  req_socket.send(request.to_message())
  let raw_reply = req_socket.recv()
  ZapReply::from_message(raw_reply)
}
