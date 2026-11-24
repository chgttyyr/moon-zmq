///|
/// ZeroMQ `inproc://` zero-copy in-memory transport engine, unified `Endpoint`
/// URI parser, and per-`Context` endpoint registry supporting both bind-first
/// and ZeroMQ's connect-before-bind semantics.

///|
/// Parsed ZeroMQ transport endpoint URI.
pub(all) enum Endpoint {
  /// TCP network endpoint (`tcp://host:port`).
  Tcp(String, Int32)
  /// In-process memory transport endpoint (`inproc://name`).
  Inproc(String)
} derive(Eq, Debug)

///|
pub extend Endpoint with Eq::{not_equal, equal}

///|
pub extend Endpoint with @debug.Debug::{to_repr}

///|
pub extend Endpoint with Show::{to_string, output}

///|
pub impl Show for Endpoint with fn output(self, logger) {
  match self {
    Tcp(host, port) => logger.write_string("tcp://\{host}:\{port}")
    Inproc(name) => logger.write_string("inproc://\{name}")
  }
}

///|
/// Validate and extract `<name>` from an `"inproc://<name>"` endpoint string.
/// Per ZeroMQ specification, inproc names must be non-empty, at most 255 bytes,
/// and must not contain NUL (`\x00`) characters.
pub fn parse_inproc_endpoint(endpoint : String) -> String raise ZmqError {
  if !endpoint.has_prefix("inproc://") {
    raise InvalidEndpoint(
      "expected inproc:// scheme in endpoint, got: \{endpoint}",
    )
  }
  let name = endpoint[9:].to_owned()
  if name.is_empty() {
    raise InvalidEndpoint("inproc:// endpoint name cannot be empty")
  }
  if name.length() > 255 {
    let n = name.length()
    raise InvalidEndpoint(
      "inproc:// endpoint name exceeds 255 characters (\{n})",
    )
  }
  for i = 0; i < name.length(); i = i + 1 {
    if name.code_unit_at(i).to_int() == 0 {
      raise InvalidEndpoint("inproc:// endpoint name cannot contain NUL byte")
    }
  }
  name
}

///|
/// Parse any supported ZeroMQ endpoint string (`tcp://...` or `inproc://...`)
/// into a structured `Endpoint` value.
pub fn Endpoint::parse(
  endpoint : String,
  for_bind~ : Bool = false,
) -> Endpoint raise ZmqError {
  if endpoint.has_prefix("inproc://") {
    let name = parse_inproc_endpoint(endpoint)
    return Inproc(name)
  }
  if endpoint.has_prefix("tcp://") {
    let rest = endpoint[6:].to_owned()
    let mut colon_idx = -1
    for i = rest.length() - 1; i >= 0; i = i - 1 {
      if rest.code_unit_at(i).to_int() == 58 {
        colon_idx = i
        break
      }
    }
    if colon_idx <= 0 || colon_idx >= rest.length() - 1 {
      raise InvalidEndpoint("missing host or port in endpoint: \{endpoint}")
    }
    let raw_host = rest[:colon_idx].to_owned()
    let port_str = rest[colon_idx + 1:].to_owned()
    let port = @string.parse_int(port_str) catch {
      _ => raise InvalidEndpoint("invalid port in endpoint: \{endpoint}")
    }
    if port < 0 || port > 65535 {
      raise InvalidEndpoint("port out of range in endpoint: \{endpoint}")
    }
    if !for_bind && port == 0 {
      raise InvalidEndpoint("cannot connect to port 0 in endpoint: \{endpoint}")
    }
    return Tcp(raw_host, port)
  }
  raise InvalidEndpoint(
    "unsupported transport scheme in endpoint: \{endpoint} (supported: tcp://, inproc://)",
  )
}

///|
/// Full-duplex in-memory byte stream backing one side of an `inproc://` connection.
pub struct InprocStream {
  rx : @aqueue.Queue[Bytes?]
  tx : @aqueue.Queue[Bytes?]
  mut bytes_written : Int64
  mut bytes_read : Int64
  mut closed : Bool
}

///|
/// Create a connected pair `(client_stream, server_stream)` of in-memory full-duplex streams.
pub fn create_inproc_stream_pair(
  queue_capacity~ : Int32 = 2048,
) -> (InprocStream, InprocStream) {
  let cap = if queue_capacity <= 0 { 64 } else { queue_capacity }
  let c2s : @aqueue.Queue[Bytes?] = @aqueue.Queue(kind=Blocking(cap))
  let s2c : @aqueue.Queue[Bytes?] = @aqueue.Queue(kind=Blocking(cap))
  let client : InprocStream = {
    rx: s2c,
    tx: c2s,
    bytes_written: 0L,
    bytes_read: 0L,
    closed: false,
  }
  let server : InprocStream = {
    rx: c2s,
    tx: s2c,
    bytes_written: 0L,
    bytes_read: 0L,
    closed: false,
  }
  (client, server)
}

///|
/// Write wire bytes into the outbound half of this `InprocStream`.
pub async fn InprocStream::write(
  self : InprocStream,
  data : Bytes,
) -> Unit raise ZmqError {
  if self.closed {
    raise Closed
  }
  if data.length() == 0 {
    return
  }
  self.tx.put(Some(data)) catch {
    _ => raise IoError("inproc peer stream closed during write")
  }
  self.bytes_written = self.bytes_written + data.length().to_int64()
}

///|
/// Read the next wire byte chunk from the inbound half of this `InprocStream`,
/// returning `None` on clean EOF.
pub async fn InprocStream::read(self : InprocStream) -> Bytes? raise ZmqError {
  if self.closed {
    return None
  }
  let item = self.rx.get() catch {
    _ => if @async.is_being_cancelled() { raise Closed } else { return None }
  }
  match item {
    Some(b) => {
      self.bytes_read = self.bytes_read + b.length().to_int64()
      Some(b)
    }
    None => {
      self.closed = true
      None
    }
  }
}

///|
/// Close this `InprocStream` and signal EOF to the remote peer without destroying
/// unread chunks still queued in transit.
pub fn InprocStream::close(self : InprocStream) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  let _ = self.tx.try_put(None) catch { _ => false }
}

///|
/// Active listener slot inside an `InprocRegistry`.
priv struct InprocListenerSlot {
  name : String
  accept_queue : @aqueue.Queue[InprocStream]
  mut active : Bool
}

///|
/// Waiter registered when `connect("inproc://...")` is called before `bind("inproc://...")`.
priv struct InprocPendingConnect {
  name : String
  ready_signal : @aqueue.Queue[Int32]
}

///|
/// Registry coordinating `inproc://` listeners and connectors within a `Context`
/// (or across standalone sockets via the default global registry).
pub struct InprocRegistry {
  priv listeners : Array[InprocListenerSlot]
  priv pending_waiters : Array[InprocPendingConnect]
  priv mut closed : Bool
}

///|
/// Create a new isolated `InprocRegistry`.
pub fn InprocRegistry::new() -> InprocRegistry {
  { listeners: [], pending_waiters: [], closed: false, }
}

///|
/// Default process-wide `InprocRegistry` used by standalone `Socket::new(...)` instances
/// created outside an explicit `Context`.
let default_inproc_registry : InprocRegistry = InprocRegistry::new()

///|
/// Check whether `name` (without `"inproc://"` prefix) is currently bound in this registry.
pub fn InprocRegistry::is_bound(self : InprocRegistry, name : String) -> Bool {
  for slot in self.listeners {
    if slot.name == name && slot.active {
      return true
    }
  }
  false
}

///|
/// Return all currently active `"inproc://<name>"` endpoints in this registry.
pub fn InprocRegistry::bound_endpoints(self : InprocRegistry) -> Array[String] {
  let out : Array[String] = []
  for slot in self.listeners {
    if slot.active {
      out.push("inproc://\{slot.name}")
    }
  }
  out
}

///|
fn InprocRegistry::register_listener(
  self : InprocRegistry,
  name : String,
) -> InprocListenerSlot raise ZmqError {
  if self.closed {
    raise Closed
  }
  for slot in self.listeners {
    if slot.name == name && slot.active {
      raise IoError("inproc://\{name} address already in use")
    }
  }
  // Remove any inactive remnant slot for the same name
  let mut i = 0
  while i < self.listeners.length() {
    if self.listeners[i].name == name {
      let _ = self.listeners.remove(i)
    } else {
      i = i + 1
    }
  }
  let slot : InprocListenerSlot = {
    name,
    accept_queue: @aqueue.Queue(kind=Unbounded),
    active: true,
  }
  self.listeners.push(slot)
  // Wake any pending early-connect waiters waiting on `name`
  let mut w = 0
  while w < self.pending_waiters.length() {
    if self.pending_waiters[w].name == name {
      let waiter = self.pending_waiters.remove(w)
      let _ = waiter.ready_signal.try_put(1) catch { _ => false }
    } else {
      w = w + 1
    }
  }
  slot
}

///|
/// Unbind an `inproc://` endpoint name from this registry. Returns `true` if removed.
pub fn InprocRegistry::unbind(self : InprocRegistry, name : String) -> Bool {
  let normalized = if name.has_prefix("inproc://") {
    name[9:].to_owned()
  } else {
    name
  }
  for i = 0; i < self.listeners.length(); i = i + 1 {
    if self.listeners[i].name == normalized {
      let slot = self.listeners.remove(i)
      slot.active = false
      slot.accept_queue.close(clear=true)
      return true
    }
  }
  false
}

///|
fn InprocRegistry::try_dial(
  self : InprocRegistry,
  name : String,
) -> InprocStream? {
  if self.closed {
    return None
  }
  for slot in self.listeners {
    if slot.name == name && slot.active {
      let (client_stream, server_stream) = create_inproc_stream_pair()
      let ok = slot.accept_queue.try_put(server_stream) catch { _ => false }
      if ok {
        return Some(client_stream)
      } else {
        client_stream.close()
        server_stream.close()
      }
    }
  }
  None
}

///|
async fn InprocRegistry::wait_for_bind(
  self : InprocRegistry,
  name : String,
  timeout_ms : Int32,
) -> Unit {
  if self.is_bound(name) || self.closed {
    return
  }
  let sig : @aqueue.Queue[Int32] = @aqueue.Queue(kind=Unbounded)
  self.pending_waiters.push({ name, ready_signal: sig, })
  let step = if timeout_ms <= 0 {
    10
  } else if timeout_ms < 25 {
    timeout_ms
  } else {
    25
  }
  let mut waited = 0
  while waited < timeout_ms && !self.closed && !@async.is_being_cancelled() {
    match sig.try_get() {
      Some(_) => return
      None => ()
    }
    if self.is_bound(name) {
      return
    }
    @async.sleep(step)
    waited = waited + step
  }
}

///|
/// Shut down all registered `inproc://` listeners and wake all pending waiters.
pub fn InprocRegistry::close_all(self : InprocRegistry) -> Unit {
  self.closed = true
  for slot in self.listeners {
    slot.active = false
    slot.accept_queue.close(clear=true)
  }
  self.listeners.clear()
  for waiter in self.pending_waiters {
    waiter.ready_signal.close(clear=true)
  }
  self.pending_waiters.clear()
}

///|
/// Perform ZMTP 3.1 `NULL` handshake over an `InprocStream`.
async fn perform_inproc_null_handshake(
  conn : InprocStream,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  let local_greet = Greeting::new(mechanism="NULL", as_server=false).encode()
  let local_meta = Metadata::new(
    local_type.to_zmtp_name(),
    identity=options.identity,
  )
  let local_ready = ZmtpCommand::Ready(local_meta).encode_frame()
  conn.write(concat_bytes([local_greet, local_ready]))
  let greet_buf : Array[Byte] = Array(capacity=64)
  let leftover : Array[Byte] = []
  while greet_buf.length() < 64 {
    match conn.read() {
      Some(b) => {
        if b.length() == 0 {
          raise ProtocolError("inproc closed before 64-byte greeting")
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
      None => raise ProtocolError("inproc EOF while reading peer greeting")
    }
  }
  let peer_greet = Greeting::decode(Bytes::from_array(greet_buf))
  if peer_greet.mechanism != "NULL" {
    let reason = "inproc mechanism mismatch: expected NULL, got \{peer_greet.mechanism}"
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
  let mut peer_meta : Metadata? = None
  let mut idx = 0
  while peer_meta is None {
    while idx < pending_items.length() && peer_meta is None {
      let item = pending_items[idx]
      idx = idx + 1
      match item {
        CommandFrame(body) => {
          let cmd = ZmtpCommand::decode_body(body)
          match cmd {
            Ready(m) =>
              match SocketType::from_zmtp_name(m.socket_type) {
                Some(peer_st) =>
                  if local_type.is_compatible_with(peer_st) {
                    peer_meta = Some(m)
                  } else {
                    let reason = "incompatible socket types: \{local_type.to_zmtp_name()} vs \{m.socket_type}"
                    conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
                      _ => ()
                    }
                    raise InvalidSocketPair(
                      local_type.to_zmtp_name(),
                      m.socket_type,
                    )
                  }
                None =>
                  raise ProtocolError("unknown Socket-Type: \{m.socket_type}")
              }
            ErrorCmd(reason) =>
              raise ProtocolError("inproc peer rejected handshake: \{reason}")
            _ =>
              raise ProtocolError("unexpected command before READY on inproc")
          }
        }
        DataMessage(_) =>
          raise ProtocolError("data frame received before READY on inproc")
      }
    }
    if peer_meta is None {
      match conn.read() {
        Some(b) =>
          for item in decoder.feed(b) {
            pending_items.push(item)
          }
        None => raise ProtocolError("inproc EOF before READY command")
      }
    }
  }
  let remaining : Array[DecodedItem] = []
  while idx < pending_items.length() {
    remaining.push(pending_items[idx])
    idx = idx + 1
  }
  match peer_meta {
    Some(m) => { peer_meta: m, decoder, initial_items: remaining, }
    None => raise ProtocolError("inproc handshake failed to produce metadata")
  }
}

///|
/// Perform ZMTP 3.1 `PLAIN` (RFC 24) handshake over an `InprocStream`.
async fn perform_inproc_plain_handshake(
  conn : InprocStream,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  let is_server = options.plain_server
  let local_greet = Greeting::new(mechanism="PLAIN", as_server=is_server).encode()
  conn.write(local_greet)
  let greet_buf : Array[Byte] = Array(capacity=64)
  let leftover : Array[Byte] = []
  while greet_buf.length() < 64 {
    match conn.read() {
      Some(b) => {
        let needed = 64 - greet_buf.length()
        for i = 0; i < b.length(); i = i + 1 {
          if i < needed {
            greet_buf.push(b[i])
          } else {
            leftover.push(b[i])
          }
        }
      }
      None => raise ProtocolError("inproc EOF during PLAIN greeting")
    }
  }
  let peer_greet = Greeting::decode(Bytes::from_array(greet_buf))
  if peer_greet.mechanism != "PLAIN" {
    raise AuthFailed(
      "expected PLAIN mechanism on inproc, got \{peer_greet.mechanism}",
    )
  }
  if peer_greet.as_server == is_server {
    raise ProtocolError(
      "PLAIN role conflict on inproc: both sides have same as-server role",
    )
  }
  let decoder = FrameDecoder::new(max_frame_size=options.max_frame_size)
  let pending : Array[DecodedItem] = []
  if !leftover.is_empty() {
    for it in decoder.feed(Bytes::from_array(leftover)) {
      pending.push(it)
    }
  }
  let mut idx = 0
  let read_next_cmd = async fn() -> Bytes raise ZmqError {
    while idx >= pending.length() {
      match conn.read() {
        Some(b) =>
          for it in decoder.feed(b) {
            pending.push(it)
          }
        None => raise ProtocolError("inproc EOF during PLAIN handshake")
      }
    }
    let item = pending[idx]
    idx = idx + 1
    match item {
      CommandFrame(body) => body
      DataMessage(_) =>
        raise ProtocolError("unexpected data frame during PLAIN handshake")
    }
  }
  let local_meta = Metadata::new(
    local_type.to_zmtp_name(),
    identity=options.identity,
  )
  let peer_meta = if is_server {
    let hello_body = read_next_cmd()
    let (user, pass) = decode_plain_hello(hello_body)
    let allowed = match options.zap_authenticator {
      Some(cb) => cb(user, pass)
      None => true
    }
    if !allowed {
      let reason = "PLAIN authentication failed"
      conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
        _ => ()
      }
      raise AuthFailed(reason)
    }
    conn.write(encode_plain_welcome())
    let init_body = read_next_cmd()
    let m = decode_plain_initiate(init_body)
    match SocketType::from_zmtp_name(m.socket_type) {
      Some(peer_st) =>
        if !local_type.is_compatible_with(peer_st) {
          raise InvalidSocketPair(local_type.to_zmtp_name(), m.socket_type)
        }
      None => raise ProtocolError("unknown Socket-Type: \{m.socket_type}")
    }
    conn.write(ZmtpCommand::Ready(local_meta).encode_frame())
    m
  } else {
    let (user, pass) = match options.plain_credentials {
      Some(creds) => creds
      None => ("", "")
    }
    conn.write(encode_plain_hello(user, pass))
    let welcome_body = read_next_cmd()
    if !is_plain_welcome(welcome_body) {
      let cmd = ZmtpCommand::decode_body(welcome_body)
      match cmd {
        ErrorCmd(r) => raise AuthFailed(r)
        _ => raise AuthFailed("expected PLAIN WELCOME from inproc server")
      }
    }
    conn.write(encode_plain_initiate(local_meta))
    let ready_body = read_next_cmd()
    let cmd = ZmtpCommand::decode_body(ready_body)
    match cmd {
      Ready(m) => {
        match SocketType::from_zmtp_name(m.socket_type) {
          Some(peer_st) =>
            if !local_type.is_compatible_with(peer_st) {
              raise InvalidSocketPair(local_type.to_zmtp_name(), m.socket_type)
            }
          None => raise ProtocolError("unknown Socket-Type: \{m.socket_type}")
        }
        m
      }
      ErrorCmd(r) => raise AuthFailed(r)
      _ => raise ProtocolError("expected READY command after INITIATE")
    }
  }
  let remaining : Array[DecodedItem] = []
  while idx < pending.length() {
    remaining.push(pending[idx])
    idx = idx + 1
  }
  { peer_meta, decoder, initial_items: remaining, }
}

///|
/// Run an established `InprocStream` peer connection inside `TransportHub`.
async fn TransportHub::run_inproc_peer_connection(
  self : TransportHub,
  group : @async.TaskGroup,
  conn : InprocStream,
  accepted_from_listener~ : Bool = false,
) -> Unit {
  defer conn.close()
  if self.closed {
    return
  }
  if self.socket_type == Pair && self.active_peer_count() >= 1 {
    return
  }
  let hs_res : Result[HandshakeResult, ZmqError] = try {
    if self.options.plain_server || self.options.plain_credentials is Some(_) {
      Ok(perform_inproc_plain_handshake(conn, self.socket_type, self.options))
    } else {
      Ok(perform_inproc_null_handshake(conn, self.socket_type, self.options))
    }
  } catch {
    err => Err(err)
  }
  let hs = match hs_res {
    Ok(h) => h
    Err(err) => {
      if @async.is_being_cancelled() {
        return
      }
      self.handshake_failures = self.handshake_failures + 1
      let reason = match err {
        AuthFailed(r) => r
        ProtocolError(r) => r
        _ => "inproc handshake failed"
      }
      self.last_auth_error = Some(reason)
      self.emit_event(HandshakeFailed(reason))
      return
    }
  }
  if self.closed || (self.socket_type == Pair && self.active_peer_count() >= 1) {
    return
  }
  let pipe_id = self.next_pipe_id
  self.next_pipe_id = self.next_pipe_id + 1
  let pipe = Pipe::new(
    pipe_id,
    hs.peer_meta,
    sndhwm=self.options.sndhwm,
    rcvhwm=self.options.rcvhwm,
  )
  self.pipes.push(pipe)
  if accepted_from_listener {
    self.emit_event(Accepted(pipe_id))
  } else {
    self.emit_event(Connected(pipe_id))
  }
  self.emit_event(
    HandshakeSucceeded(hs.peer_meta.socket_type, pipe.peer_identity),
  )
  let _ = self.pipe_ready_notify.try_put(pipe_id) catch { _ => false }

  let writer_task = group.spawn(no_wait=true, allow_failure=true, () => {
    while !self.closed && pipe.is_open() && !@async.is_being_cancelled() {
      match pipe.dequeue_outbound() {
        Some(wire) => {
          let _ = self.pipe_ready_notify.try_put(pipe_id) catch { _ => false }
          conn.write(wire) catch {
            _ => {
              pipe.close()
              break
            }
          }
        }
        None => break
      }
    }
  })
  self.tasks.push(writer_task)

  self.send_initial_subscriptions(pipe)

  for item in hs.initial_items {
    if !self.handle_decoded_item(pipe, item) {
      self.detach_pipe(pipe_id)
      writer_task.cancel()
      return
    }
  }

  let decoder = hs.decoder
  while !self.closed && pipe.is_open() && !@async.is_being_cancelled() {
    let chunk = conn.read() catch { _ => None }
    if @async.is_being_cancelled() {
      break
    }
    match chunk {
      Some(b) => {
        if b.length() == 0 {
          continue
        }
        let decoded_res : Result[Array[DecodedItem], ZmqError] = Ok(
          decoder.feed(b),
        ) catch {
          err => Err(err)
        }
        match decoded_res {
          Ok(items) => {
            let mut keep_going = true
            for item in items {
              if !self.handle_decoded_item(pipe, item) {
                keep_going = false
                break
              }
            }
            if !keep_going {
              break
            }
          }
          Err(_) => break
        }
      }
      None => break
    }
  }
  self.detach_pipe(pipe_id)
  writer_task.cancel()
}

///|
/// Bind an `"inproc://<name>"` endpoint inside `registry` and accept in-memory peer sessions.
pub fn TransportHub::bind_inproc(
  self : TransportHub,
  group : @async.TaskGroup,
  endpoint : String,
  registry~ : InprocRegistry = default_inproc_registry,
) -> String raise ZmqError {
  if self.closed {
    raise Closed
  }
  let name = parse_inproc_endpoint(endpoint)
  let slot = registry.register_listener(name)
  let bound_uri = "inproc://\{name}"
  let accept_task = group.spawn(no_wait=true, allow_failure=true, () => {
    defer {
      let _ = registry.unbind(name)
    }
    while !self.closed && slot.active && !@async.is_being_cancelled() {
      let accepted = slot.accept_queue.get() catch { _ => break }
      if self.closed || @async.is_being_cancelled() {
        accepted.close()
        break
      }
      let conn_task = group.spawn(no_wait=true, allow_failure=true, () => {
        self.run_inproc_peer_connection(
          group,
          accepted,
          accepted_from_listener=true,
        )
      })
      self.tasks.push(conn_task)
    }
  })
  self.tasks.push(accept_task)
  self.emit_event(Listening(bound_uri))
  bound_uri
}

///|
/// Connect to an `"inproc://<name>"` endpoint inside `registry`.
/// Supports connecting *before* the peer calls `bind("inproc://<name>")` as well as
/// automatic reconnection if the bound listener restarts.
pub fn TransportHub::connect_inproc(
  self : TransportHub,
  group : @async.TaskGroup,
  endpoint : String,
  registry~ : InprocRegistry = default_inproc_registry,
) -> Unit raise ZmqError {
  if self.closed {
    raise Closed
  }
  let name = parse_inproc_endpoint(endpoint)
  let connect_task = group.spawn(no_wait=true, allow_failure=true, () => {
    let mut delay_ms = if self.options.reconnect_ivl_ms <= 0 {
      10
    } else {
      self.options.reconnect_ivl_ms
    }
    let mut first_attempt = true
    while !self.closed && !@async.is_being_cancelled() {
      if !first_attempt {
        self.reconnect_attempts = self.reconnect_attempts + 1
        registry.wait_for_bind(name, delay_ms)
        delay_ms = next_reconnect_delay_ms(
          delay_ms,
          self.options.reconnect_ivl_ms,
          self.options.reconnect_ivl_max_ms,
        )
      }
      first_attempt = false
      if self.closed || @async.is_being_cancelled() {
        break
      }
      match registry.try_dial(name) {
        Some(client_stream) => {
          let prev_peers = self.next_pipe_id
          self.run_inproc_peer_connection(
            group,
            client_stream,
            accepted_from_listener=false,
          )
          if self.next_pipe_id > prev_peers {
            delay_ms = if self.options.reconnect_ivl_ms <= 0 {
              10
            } else {
              self.options.reconnect_ivl_ms
            }
          }
        }
        None => ()
      }
    }
  })
  self.tasks.push(connect_task)
}
