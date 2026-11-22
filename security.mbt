///|
/// RFC 24 PLAIN Security Mechanism Implementation for ZMTP 3.1.
///
/// Handshake flow:
/// Client                             Server
///   | ----- Greeting (PLAIN, as-server=0) ------> |
///   | <---- Greeting (PLAIN, as-server=1) ------- |
///   | ----- HELLO (username, password) ---------> |
///   | <---- WELCOME (or ERROR) ------------------ |
///   | ----- INITIATE (metadata) ----------------> |
///   | <---- READY (metadata) -------------------- |

///|
/// Encode a PLAIN `HELLO` command body (RFC 24 Section 3.1).
/// Format: `\x05HELLO` + `[username_len: 1B]` + `username` + `[password_len: 1B]` + `password`
pub fn encode_plain_hello(username : String, password : String) -> Bytes {
  let u_bytes = string_to_ascii_bytes(username)
  let p_bytes = string_to_ascii_bytes(password)
  let u_len = if u_bytes.length() > 255 { 255 } else { u_bytes.length() }
  let p_len = if p_bytes.length() > 255 { 255 } else { p_bytes.length() }

  let out : Array[Byte] = Array(capacity=8 + u_len + p_len)
  out.push(b'\x05')
  out.push(b'H')
  out.push(b'E')
  out.push(b'L')
  out.push(b'L')
  out.push(b'O')
  out.push(u_len.to_byte())
  for i = 0; i < u_len; i = i + 1 {
    out.push(u_bytes[i])
  }
  out.push(p_len.to_byte())
  for i = 0; i < p_len; i = i + 1 {
    out.push(p_bytes[i])
  }
  Frame::command_frame(Bytes::from_array(out)).encode()
}

///|
/// Decode a PLAIN `HELLO` command body into `(username, password)` (RFC 24 Section 3.1).
pub fn decode_plain_hello(body : Bytes) -> (String, String) raise ZmqError {
  if body.length() < 8 {
    raise ProtocolError("PLAIN HELLO body too short")
  }
  if body[0].to_int() != 5 ||
    body[1].to_int() != 72 ||
    body[2].to_int() != 69 ||
    body[3].to_int() != 76 ||
    body[4].to_int() != 76 ||
    body[5].to_int() != 79 {
    raise ProtocolError("invalid PLAIN HELLO command prefix")
  }
  let u_len = body[6].to_int()
  if 7 + u_len >= body.length() {
    raise ProtocolError("truncated username in PLAIN HELLO")
  }
  let u_bytes = slice_bytes(body, 7, 7 + u_len)
  let p_len = body[7 + u_len].to_int()
  let p_start = 8 + u_len
  if p_start + p_len > body.length() {
    raise ProtocolError("truncated password in PLAIN HELLO")
  }
  let p_bytes = slice_bytes(body, p_start, p_start + p_len)
  (ascii_bytes_to_string(u_bytes), ascii_bytes_to_string(p_bytes))
}

///|
/// Encode a PLAIN `WELCOME` command frame (RFC 24 Section 3.2).
pub fn encode_plain_welcome() -> Bytes {
  let out : Array[Byte] = [b'\x07', b'W', b'E', b'L', b'C', b'O', b'M', b'E']
  Frame::command_frame(Bytes::from_array(out)).encode()
}

///|
/// Check if a decoded command body is a PLAIN `WELCOME` command.
pub fn is_plain_welcome(body : Bytes) -> Bool {
  if body.length() != 8 {
    return false
  }
  body[0].to_int() == 7 &&
  body[1].to_int() == 87 && // W
  body[2].to_int() == 69 && // E
  body[3].to_int() == 76 && // L
  body[4].to_int() == 67 && // C
  body[5].to_int() == 79 && // O
  body[6].to_int() == 77 && // M
  body[7].to_int() == 69 // E
}

///|
/// Encode a PLAIN `INITIATE` command frame carrying client metadata (RFC 24 Section 3.3).
pub fn encode_plain_initiate(meta : Metadata) -> Bytes {
  let meta_bytes = meta.encode()
  let out : Array[Byte] = Array(capacity=9 + meta_bytes.length())
  out.push(b'\x08')
  out.push(b'I')
  out.push(b'N')
  out.push(b'I')
  out.push(b'T')
  out.push(b'I')
  out.push(b'A')
  out.push(b'T')
  out.push(b'E')
  for i = 0; i < meta_bytes.length(); i = i + 1 {
    out.push(meta_bytes[i])
  }
  Frame::command_frame(Bytes::from_array(out)).encode()
}

///|
/// Decode a PLAIN `INITIATE` command body into `Metadata`.
pub fn decode_plain_initiate(body : Bytes) -> Metadata raise ZmqError {
  if body.length() < 9 {
    raise ProtocolError("PLAIN INITIATE body too short")
  }
  let meta_bytes = slice_bytes(body, 9, body.length())
  Metadata::decode(meta_bytes)
}

///|
/// Perform client or server RFC 24 PLAIN authentication handshake.
async fn perform_plain_handshake(
  conn : @socket.Tcp,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  if options.plain_server {
    perform_plain_handshake_server(conn, local_type, options)
  } else {
    perform_plain_handshake_client(conn, local_type, options)
  }
}

///|
/// Server-side PLAIN handshake implementation.
async fn perform_plain_handshake_server(
  conn : @socket.Tcp,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  let server_greet = Greeting::new(mechanism="PLAIN", as_server=true).encode()
  conn.write(server_greet) catch {
    _ => raise IoError("server failed writing PLAIN greeting")
  }

  // Read peer greeting
  let greet_buf : Array[Byte] = Array(capacity=64)
  let leftover : Array[Byte] = []
  while greet_buf.length() < 64 {
    let chunk = conn.read() catch {
      _ => raise IoError("server failed reading client greeting")
    }
    match chunk {
      Some(b) => {
        if b.length() == 0 {
          raise ProtocolError("connection closed before greeting")
        }
        let needed = 64 - greet_buf.length()
        for i = 0; i < b.length(); i = i + 1 {
          if i < needed {
            greet_buf.push(b[i])
          } else {
            leftover.push(b[i])
          }
        }
      }
      None => raise ProtocolError("EOF reading client greeting")
    }
  }

  let peer_greet = Greeting::decode(Bytes::from_array(greet_buf))
  if peer_greet.mechanism != "PLAIN" {
    let reason = "security mechanism mismatch: expected PLAIN, got \{peer_greet.mechanism}"
    conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
      _ => ()
    }
    raise AuthFailed(reason)
  }

  let decoder = FrameDecoder::new(max_frame_size=options.max_frame_size)
  let pending_items : Array[DecodedItem] = []
  if !leftover.is_empty() {
    for item in decoder.feed(Bytes::from_array(leftover)) {
      pending_items.push(item)
    }
  }

  // Step 1: Await HELLO command from client
  let mut creds : (String, String)? = None
  let mut idx = 0
  while creds is None {
    while idx < pending_items.length() && creds is None {
      let item = pending_items[idx]
      idx = idx + 1
      match item {
        CommandFrame(body) => {
          let (u, p) = decode_plain_hello(body) catch {
            _ => raise ProtocolError("expected PLAIN HELLO command from client")
          }
          creds = Some((u, p))
        }
        _ => raise ProtocolError("unexpected data frame before PLAIN HELLO")
      }
    }
    if creds is None {
      let chunk = conn.read() catch {
        _ => raise IoError("failed reading HELLO frame")
      }
      match chunk {
        Some(b) => {
          if b.length() == 0 {
            raise ProtocolError("EOF before PLAIN HELLO")
          }
          for item in decoder.feed(b) {
            pending_items.push(item)
          }
        }
        None => raise ProtocolError("EOF before PLAIN HELLO")
      }
    }
  }

  let (username, password) = creds.unwrap()

  // Validate credentials with ZAP authenticator callback if configured
  let is_auth_ok = match options.zap_authenticator {
    Some(auth) => auth(username, password)
    None => true
  }

  if !is_auth_ok {
    let err_frame = ZmtpCommand::ErrorCmd(
      "authentication failed: 400 Invalid credentials",
    ).encode_frame()
    conn.write(err_frame) catch {
      _ => ()
    }
    raise AuthFailed("authentication failed for user '\{username}'")
  }

  // Send WELCOME to client
  conn.write(encode_plain_welcome()) catch {
    _ => raise IoError("failed writing PLAIN WELCOME")
  }

  // Step 2: Await INITIATE command from client
  let mut peer_meta : Metadata? = None
  while peer_meta is None {
    while idx < pending_items.length() && peer_meta is None {
      let item = pending_items[idx]
      idx = idx + 1
      match item {
        CommandFrame(body) => {
          let meta = decode_plain_initiate(body) catch {
            _ =>
              raise ProtocolError("expected PLAIN INITIATE command from client")
          }
          match SocketType::from_zmtp_name(meta.socket_type) {
            Some(peer_st) =>
              if local_type.is_compatible_with(peer_st) {
                peer_meta = Some(meta)
              } else {
                let reason = "incompatible socket types: \{local_type.to_zmtp_name()} vs \{meta.socket_type}"
                conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
                  _ => ()
                }
                raise InvalidSocketPair(
                  local_type.to_zmtp_name(),
                  meta.socket_type,
                )
              }
            None =>
              raise ProtocolError(
                "unknown client Socket-Type: \{meta.socket_type}",
              )
          }
        }
        _ => raise ProtocolError("unexpected data frame before PLAIN INITIATE")
      }
    }
    if peer_meta is None {
      let chunk = conn.read() catch {
        _ => raise IoError("failed reading INITIATE frame")
      }
      match chunk {
        Some(b) => {
          if b.length() == 0 {
            raise ProtocolError("EOF before PLAIN INITIATE")
          }
          for item in decoder.feed(b) {
            pending_items.push(item)
          }
        }
        None => raise ProtocolError("EOF before PLAIN INITIATE")
      }
    }
  }

  // Send READY command to client
  let local_meta = Metadata::new(
    local_type.to_zmtp_name(),
    identity=options.identity,
  )
  conn.write(ZmtpCommand::Ready(local_meta).encode_frame()) catch {
    _ => raise IoError("failed writing READY command")
  }

  let remaining : Array[DecodedItem] = []
  while idx < pending_items.length() {
    remaining.push(pending_items[idx])
    idx = idx + 1
  }

  { peer_meta: peer_meta.unwrap(), decoder, initial_items: remaining, }
}

///|
/// Client-side PLAIN handshake implementation.
async fn perform_plain_handshake_client(
  conn : @socket.Tcp,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  let (username, password) = match options.plain_credentials {
    Some(cp) => cp
    None => ("", "")
  }

  let client_greet = Greeting::new(mechanism="PLAIN", as_server=false).encode()
  let hello_wire = encode_plain_hello(username, password)
  conn.write(concat_bytes([client_greet, hello_wire])) catch {
    _ => raise IoError("failed writing PLAIN greeting and HELLO")
  }

  // Read server greeting (64 bytes)
  let greet_buf : Array[Byte] = Array(capacity=64)
  let leftover : Array[Byte] = []
  while greet_buf.length() < 64 {
    let chunk = conn.read() catch {
      _ => raise IoError("failed reading server PLAIN greeting")
    }
    match chunk {
      Some(b) => {
        if b.length() == 0 {
          raise ProtocolError("connection closed before greeting")
        }
        let needed = 64 - greet_buf.length()
        for i = 0; i < b.length(); i = i + 1 {
          if i < needed {
            greet_buf.push(b[i])
          } else {
            leftover.push(b[i])
          }
        }
      }
      None => raise ProtocolError("EOF reading server greeting")
    }
  }

  let server_greet = Greeting::decode(Bytes::from_array(greet_buf))
  if server_greet.mechanism != "PLAIN" {
    let reason = "server mechanism mismatch: expected PLAIN, got \{server_greet.mechanism}"
    raise AuthFailed(reason)
  }

  let decoder = FrameDecoder::new(max_frame_size=options.max_frame_size)
  let pending_items : Array[DecodedItem] = []
  if !leftover.is_empty() {
    for item in decoder.feed(Bytes::from_array(leftover)) {
      pending_items.push(item)
    }
  }

  // Step 1: Wait for WELCOME command (or ERROR)
  let mut welcome_received = false
  let mut idx = 0
  while !welcome_received {
    while idx < pending_items.length() && !welcome_received {
      let item = pending_items[idx]
      idx = idx + 1
      match item {
        CommandFrame(body) =>
          if is_plain_welcome(body) {
            welcome_received = true
          } else {
            let cmd = ZmtpCommand::decode_body(body) catch {
              _ => raise ProtocolError("unknown command instead of WELCOME")
            }
            match cmd {
              ErrorCmd(reason) =>
                raise AuthFailed("server rejected authentication: \{reason}")
              _ => raise ProtocolError("unexpected command instead of WELCOME")
            }
          }
        _ => raise ProtocolError("unexpected data frame before WELCOME")
      }
    }
    if !welcome_received {
      let chunk = conn.read() catch {
        _ => raise IoError("failed reading WELCOME frame")
      }
      match chunk {
        Some(b) => {
          if b.length() == 0 {
            raise ProtocolError("EOF before WELCOME")
          }
          for item in decoder.feed(b) {
            pending_items.push(item)
          }
        }
        None => raise ProtocolError("EOF before WELCOME")
      }
    }
  }

  // Step 2: Send INITIATE command with client metadata
  let local_meta = Metadata::new(
    local_type.to_zmtp_name(),
    identity=options.identity,
  )
  conn.write(encode_plain_initiate(local_meta)) catch {
    _ => raise IoError("failed writing PLAIN INITIATE")
  }

  // Step 3: Wait for READY command from server
  let mut peer_meta : Metadata? = None
  while peer_meta is None {
    while idx < pending_items.length() && peer_meta is None {
      let item = pending_items[idx]
      idx = idx + 1
      match item {
        CommandFrame(body) => {
          let cmd = ZmtpCommand::decode_body(body) catch {
            _ => raise ProtocolError("malformed server READY frame")
          }
          match cmd {
            Ready(meta) =>
              match SocketType::from_zmtp_name(meta.socket_type) {
                Some(peer_st) =>
                  if local_type.is_compatible_with(peer_st) {
                    peer_meta = Some(meta)
                  } else {
                    raise InvalidSocketPair(
                      local_type.to_zmtp_name(),
                      meta.socket_type,
                    )
                  }
                None =>
                  raise ProtocolError(
                    "unknown server Socket-Type: \{meta.socket_type}",
                  )
              }
            ErrorCmd(reason) =>
              raise ProtocolError("server sent ERROR: \{reason}")
            _ => raise ProtocolError("unexpected command before READY")
          }
        }
        _ => raise ProtocolError("unexpected data frame before READY")
      }
    }
    if peer_meta is None {
      let chunk = conn.read() catch {
        _ => raise IoError("failed reading READY frame")
      }
      match chunk {
        Some(b) => {
          if b.length() == 0 {
            raise ProtocolError("EOF before READY")
          }
          for item in decoder.feed(b) {
            pending_items.push(item)
          }
        }
        None => raise ProtocolError("EOF before READY")
      }
    }
  }

  let remaining : Array[DecodedItem] = []
  while idx < pending_items.length() {
    remaining.push(pending_items[idx])
    idx = idx + 1
  }

  { peer_meta: peer_meta.unwrap(), decoder, initial_items: remaining, }
}
