///|
/// Configurable options for a ZeroMQ socket and its underlying ZMTP transport.
pub struct SocketOptions {
  mut identity : Bytes?
  mut sndhwm : Int
  mut rcvhwm : Int
  mut reconnect_ivl_ms : Int
  mut reconnect_ivl_max_ms : Int
  mut handshake_timeout_ms : Int
  mut max_frame_size : Int
  mut router_mandatory : Bool
  mut xpub_verbose : Bool
  mut send_subscribe_commands : Bool
  mut plain_credentials : (String, String)?
  mut plain_server : Bool
  mut zap_authenticator : ((String, String) -> Bool)?
}

///|
/// Construct default `SocketOptions`.
pub fn SocketOptions::new() -> SocketOptions {
  {
    identity: None,
    sndhwm: DEFAULT_HWM,
    rcvhwm: DEFAULT_HWM,
    reconnect_ivl_ms: 100,
    reconnect_ivl_max_ms: 5000,
    handshake_timeout_ms: 5000,
    max_frame_size: DEFAULT_MAX_FRAME_SIZE,
    router_mandatory: false,
    xpub_verbose: false,
    send_subscribe_commands: false,
    plain_credentials: None,
    plain_server: false,
    zap_authenticator: None,
  }
}

///|
/// Set the ZMTP `Identity` (`Routing-Id`) on `SocketOptions`.
pub fn SocketOptions::set_identity(self : SocketOptions, id : Bytes?) -> Unit {
  self.identity = id
}

///|
/// Set the outbound High-Water Mark (`sndhwm`) on `SocketOptions`.
pub fn SocketOptions::set_sndhwm(self : SocketOptions, hwm : Int) -> Unit {
  self.sndhwm = hwm
}

///|
/// Set the inbound High-Water Mark (`rcvhwm`) on `SocketOptions`.
pub fn SocketOptions::set_rcvhwm(self : SocketOptions, hwm : Int) -> Unit {
  self.rcvhwm = hwm
}

///|
/// Set the reconnect initial interval (`reconnect_ivl_ms`) and max backoff (`reconnect_ivl_max_ms`).
pub fn SocketOptions::set_reconnect_interval(
  self : SocketOptions,
  ivl_ms : Int,
  max_ms? : Int = 5000,
) -> Unit {
  self.reconnect_ivl_ms = ivl_ms
  self.reconnect_ivl_max_ms = max_ms
}

///|
/// Set the handshake timeout in milliseconds (`handshake_timeout_ms`).
pub fn SocketOptions::set_handshake_timeout(
  self : SocketOptions,
  timeout_ms : Int,
) -> Unit {
  self.handshake_timeout_ms = timeout_ms
}

///|
/// Set the maximum allowed single frame size in bytes (`max_frame_size`).
pub fn SocketOptions::set_max_frame_size(
  self : SocketOptions,
  max_bytes : Int,
) -> Unit {
  self.max_frame_size = max_bytes
}

///|
/// Enable or disable `ZMQ_ROUTER_MANDATORY` behavior on a `ROUTER` socket.
pub fn SocketOptions::set_router_mandatory(
  self : SocketOptions,
  mandatory : Bool,
) -> Unit {
  self.router_mandatory = mandatory
}

///|
/// Enable or disable `ZMQ_XPUB_VERBOSE` behavior on an `XPUB` socket.
pub fn SocketOptions::set_xpub_verbose(
  self : SocketOptions,
  verbose : Bool,
) -> Unit {
  self.xpub_verbose = verbose
}

///|
/// Configure whether `SUB`/`XSUB` also emit ZMTP 3.1 `SUBSCRIBE`/`CANCEL` commands.
pub fn SocketOptions::set_send_subscribe_commands(
  self : SocketOptions,
  enabled : Bool,
) -> Unit {
  self.send_subscribe_commands = enabled
}

///|
/// Configure client-side `PLAIN` credentials (`username`, `password`).
pub fn SocketOptions::set_plain_credentials(
  self : SocketOptions,
  username : String,
  password : String,
) -> Unit {
  self.plain_credentials = Some((username, password))
  self.plain_server = false
}

///|
/// Configure server-side `PLAIN` authentication with an optional ZAP callback.
pub fn SocketOptions::set_plain_server(
  self : SocketOptions,
  enabled : Bool,
  authenticator? : ((String, String) -> Bool)? = None,
) -> Unit {
  self.plain_server = enabled
  self.zap_authenticator = authenticator
  if enabled {
    self.plain_credentials = None
  }
}

///|
/// Compute the next reconnect delay in milliseconds using exponential backoff,
/// capped at `max_ms`. If `max_ms <= initial_ms`, a fixed interval `initial_ms` is used.
pub fn next_reconnect_delay_ms(
  current_ms : Int,
  initial_ms : Int,
  max_ms : Int,
) -> Int {
  let base = if initial_ms <= 0 { 10 } else { initial_ms }
  if max_ms <= base {
    return base
  }
  let cur = if current_ms < base { base } else { current_ms }
  let doubled = cur * 2
  if doubled > max_ms {
    max_ms
  } else {
    doubled
  }
}

///|
/// Parse a ZeroMQ `"tcp://host:port"` endpoint string into a `@socket.Addr`.
/// Supports wildcard `"*"` (mapped to `0.0.0.0` on bind or `127.0.0.1` on connect)
/// and `"localhost"` (mapped to `127.0.0.1`).
pub fn parse_tcp_endpoint(
  endpoint : String,
  for_bind? : Bool = false,
) -> @socket.Addr raise ZmqError {
  if !endpoint.has_prefix("tcp://") {
    raise InvalidEndpoint(
      "unsupported transport scheme in endpoint: \{endpoint} (expected tcp://host:port)",
    )
  }
  let rest = endpoint[6:].to_owned()
  let mut colon_idx = -1
  for i = rest.length() - 1; i >= 0; i = i - 1 {
    if rest.code_unit_at(i).to_int() == 58 { // ':'
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
    raise InvalidEndpoint("port out of range (0..65535) in endpoint: \{endpoint}")
  }
  if !for_bind && port == 0 {
    raise InvalidEndpoint("cannot connect to port 0 in endpoint: \{endpoint}")
  }
  let host = if raw_host == "*" {
    if for_bind {
      "0.0.0.0"
    } else {
      "127.0.0.1"
    }
  } else if raw_host == "localhost" {
    "127.0.0.1"
  } else {
    raw_host
  }
  let normalized = "\{host}:\{port}"
  @socket.Addr::parse(normalized) catch {
    _ => raise InvalidEndpoint("invalid IPv4/IPv6 address in endpoint: \{endpoint}")
  }
}

///|
/// Pluggable handshake hook used by `TransportHub` so security mechanisms
/// (NULL in M1/M2 and PLAIN in M4) share the same transport engine.
priv struct HandshakeResult {
  peer_meta : Metadata
  decoder : FrameDecoder
  initial_items : Array[DecodedItem]
}

///|
async fn perform_null_handshake(
  conn : @socket.Tcp,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  let local_greet = Greeting::new(mechanism="NULL", as_server=false).encode()
  let local_meta = Metadata::new(
    local_type.to_zmtp_name(),
    identity=options.identity,
  )
  let local_ready = ZmtpCommand::Ready(local_meta).encode_frame()
  conn.write(concat_bytes([local_greet, local_ready])) catch {
    err => raise IoError("failed writing greeting/READY: \{err}")
  }
  // Read at least 64 bytes for the peer greeting
  let greet_buf : Array[Byte] = Array(capacity=64)
  let leftover : Array[Byte] = []
  while greet_buf.length() < 64 {
    let chunk = conn.read() catch {
      err => raise IoError("failed reading peer greeting: \{err}")
    }
    match chunk {
      Some(b) => {
        if b.length() == 0 {
          raise ProtocolError("connection closed before 64-byte greeting")
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
      None => raise ProtocolError("EOF while reading peer greeting")
    }
  }
  let peer_greet = Greeting::decode(Bytes::from_array(greet_buf))
  if peer_greet.mechanism != "NULL" {
    let reason = "security mechanism mismatch: expected NULL, got \{peer_greet.mechanism}"
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
                None => {
                  let reason = "unknown Socket-Type: \{m.socket_type}"
                  conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
                    _ => ()
                  }
                  raise ProtocolError(reason)
                }
              }
            ErrorCmd(reason) =>
              raise ProtocolError("peer rejected handshake: \{reason}")
            _ => {
              let reason = "unexpected command before READY"
              conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
                _ => ()
              }
              raise ProtocolError(reason)
            }
          }
        }
        DataMessage(_) => {
          let reason = "data frame received before READY"
          conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
            _ => ()
          }
          raise ProtocolError(reason)
        }
      }
    }
    if peer_meta is None {
      let chunk = conn.read() catch {
        err => raise IoError("failed reading READY frame: \{err}")
      }
      match chunk {
        Some(b) => {
          if b.length() == 0 {
            raise ProtocolError("EOF before READY command")
          }
          for item in decoder.feed(b) {
            pending_items.push(item)
          }
        }
        None => raise ProtocolError("EOF before READY command")
      }
    }
  }
  let remaining : Array[DecodedItem] = []
  while idx < pending_items.length() {
    remaining.push(pending_items[idx])
    idx = idx + 1
  }
  match peer_meta {
    Some(m) => { peer_meta: m, decoder, initial_items: remaining }
    None => raise ProtocolError("handshake failed to produce metadata")
  }
}

///|
/// Multi-connection transport hub managing TCP listeners (`bind`), outbound
/// connectors (`connect`) with exponential-backoff reconnection, and active `Pipe`s.
pub struct TransportHub {
  socket_type : SocketType
  options : SocketOptions
  pipes : Array[Pipe]
  tasks : Array[@async.Task[Unit]]
  mut next_pipe_id : Int
  mut rr_index : Int
  mut fq_index : Int
  inbound_notify : @aqueue.Queue[Int]
  pipe_ready_notify : @aqueue.Queue[Int]
  sub_events : @aqueue.Queue[Message]
  local_subscriptions : Array[Bytes]
  topic_refcounts : Map[Bytes, Int]
  mut reconnect_attempts : Int
  mut handshake_failures : Int
  mut last_auth_error : String?
  mut closed : Bool
}

///|
/// Create a new `TransportHub` for the given `SocketType` and `SocketOptions`.
pub fn TransportHub::new(
  socket_type : SocketType,
  options? : SocketOptions = SocketOptions::new(),
) -> TransportHub {
  {
    socket_type,
    options,
    pipes: [],
    tasks: [],
    next_pipe_id: 1,
    rr_index: 0,
    fq_index: 0,
    inbound_notify: @aqueue.Queue(kind=Unbounded),
    pipe_ready_notify: @aqueue.Queue(kind=Unbounded),
    sub_events: @aqueue.Queue(kind=Unbounded),
    local_subscriptions: [],
    topic_refcounts: {},
    reconnect_attempts: 0,
    handshake_failures: 0,
    last_auth_error: None,
    closed: false,
  }
}

///|
/// Number of currently active (handshake-completed) peer connections.
pub fn TransportHub::active_peer_count(self : TransportHub) -> Int {
  let mut count = 0
  for p in self.pipes {
    if p.is_open() {
      count = count + 1
    }
  }
  count
}

///|
/// Total number of automatic reconnection attempts made by `connect` endpoints.
pub fn TransportHub::reconnect_count(self : TransportHub) -> Int {
  self.reconnect_attempts
}

///|
/// Remove a closed pipe from the active pipes list and adjust its topic subscriptions.
fn TransportHub::detach_pipe(self : TransportHub, pipe_id : Int) -> Unit {
  let mut idx = -1
  for i = 0; i < self.pipes.length(); i = i + 1 {
    if self.pipes[i].id == pipe_id {
      idx = i
      break
    }
  }
  if idx >= 0 {
    let p = self.pipes.remove(idx)
    // Decrement subscription reference counts if this is PUB/XPUB
    for topic in p.subscriptions {
      match self.topic_refcounts.get(topic) {
        Some(c) =>
          if c <= 1 {
            self.topic_refcounts.remove(topic)
            if self.socket_type == XPub {
              let _ = self.sub_events.try_put(
                encode_subscription_frame(false, topic),
              ) catch {
                _ => false
              }
              let _ = self.inbound_notify.try_put(pipe_id) catch { _ => false }
            }
          } else {
            self.topic_refcounts[topic] = c - 1
          }
        None => ()
      }
    }
    p.close()
  }
}

///|
/// Process a single decoded item (`DataMessage` or `CommandFrame`) arriving on `pipe`.
async fn TransportHub::handle_decoded_item(
  self : TransportHub,
  pipe : Pipe,
  item : DecodedItem,
) -> Bool {
  match item {
    CommandFrame(body) => {
      let cmd = ZmtpCommand::decode_body(body) catch { _ => return false }
      match cmd {
        Ping(_ttl, ctx) => {
          let pong = ZmtpCommand::Pong(ctx).encode_frame()
          let _ = pipe.send_wire(pong, policy=DropOnFull) catch {
            _ => return false
          }
          true
        }
        Pong(_) => true
        ErrorCmd(_) => false
        Subscribe(topic) => {
          self.apply_peer_subscription(pipe, true, topic)
          true
        }
        Cancel(topic) => {
          self.apply_peer_subscription(pipe, false, topic)
          true
        }
        _ => true
      }
    }
    DataMessage(msg) => {
      // On PUB or XPUB sockets, incoming single-frame messages starting with 0x01 or 0x00
      // are ZMTP 3.0 / libzmq subscription management frames.
      if self.socket_type == Pub || self.socket_type == XPub {
        match decode_subscription_frame(msg) {
          Some((is_sub, topic)) => {
            self.apply_peer_subscription(pipe, is_sub, topic)
            return true
          }
          None =>
            if self.socket_type == XPub {
              let _ = self.sub_events.try_put(msg) catch { _ => false }
              let _ = self.inbound_notify.try_put(pipe.id) catch { _ => false }
              return true
            } else {
              return true
            }
        }
      }
      // On SUB socket, filter incoming data messages by registered topic prefixes
      if self.socket_type == Sub {
        if !self.matches_local_subscription(msg) {
          return true
        }
      }
      let ok = pipe.push_inbound(msg)
      if ok {
        let _ = self.inbound_notify.try_put(pipe.id) catch { _ => false }
      }
      ok
    }
  }
}

///|
/// Update peer subscription state on a `PUB` or `XPUB` socket and emit `XPUB` events.
fn TransportHub::apply_peer_subscription(
  self : TransportHub,
  pipe : Pipe,
  subscribe : Bool,
  topic : Bytes,
) -> Unit {
  if subscribe {
    let added_on_pipe = pipe.add_subscription(topic)
    if added_on_pipe {
      let prev = match self.topic_refcounts.get(topic) {
        Some(c) => c
        None => 0
      }
      self.topic_refcounts[topic] = prev + 1
      if self.socket_type == XPub && (prev == 0 || self.options.xpub_verbose) {
        let _ = self.sub_events.try_put(encode_subscription_frame(true, topic)) catch {
          _ => false
        }
        let _ = self.inbound_notify.try_put(pipe.id) catch { _ => false }
      }
    } else if self.socket_type == XPub && self.options.xpub_verbose {
      let _ = self.sub_events.try_put(encode_subscription_frame(true, topic)) catch {
        _ => false
      }
      let _ = self.inbound_notify.try_put(pipe.id) catch { _ => false }
    }
  } else {
    let removed_on_pipe = pipe.remove_subscription(topic)
    if removed_on_pipe {
      match self.topic_refcounts.get(topic) {
        Some(c) =>
          if c <= 1 {
            self.topic_refcounts.remove(topic)
            if self.socket_type == XPub {
              let _ = self.sub_events.try_put(
                encode_subscription_frame(false, topic),
              ) catch {
                _ => false
              }
              let _ = self.inbound_notify.try_put(pipe.id) catch { _ => false }
            }
          } else {
            self.topic_refcounts[topic] = c - 1
          }
        None => ()
      }
    }
  }
}

///|
/// Check whether `msg` matches any local subscription prefix on a `SUB` socket.
pub fn TransportHub::matches_local_subscription(
  self : TransportHub,
  msg : Message,
) -> Bool {
  let first = msg.first_bytes()
  for prefix in self.local_subscriptions {
    if bytes_starts_with(first, prefix) {
      return true
    }
  }
  false
}

///|
/// Send all currently registered local subscriptions onto a newly connected `Pipe`
/// (used by `SUB` and `XSUB` upon initial connect and after automatic reconnect).
async fn TransportHub::send_initial_subscriptions(
  self : TransportHub,
  pipe : Pipe,
) -> Unit {
  if self.socket_type != Sub && self.socket_type != XSub {
    return
  }
  for topic in self.local_subscriptions {
    let sub_wire = encode_subscription_frame(true, topic).encode()
    let _ = pipe.send_wire(sub_wire, policy=DropOnFull) catch { _ => false }
    if self.options.send_subscribe_commands {
      let cmd_wire = ZmtpCommand::Subscribe(topic).encode_frame()
      let _ = pipe.send_wire(cmd_wire, policy=DropOnFull) catch { _ => false }
    }
  }
}

///|
/// Run a single established TCP peer connection (handshake + concurrent reader/writer).
async fn TransportHub::run_peer_connection(
  self : TransportHub,
  group : @async.TaskGroup,
  conn : @socket.Tcp,
) -> Unit {
  defer conn.close()
  if self.closed {
    return
  }
  // PAIR socket exclusivity check: only one active peer allowed at a time
  if self.socket_type == Pair && self.active_peer_count() >= 1 {
    return
  }
  let hs = @async.with_timeout(self.options.handshake_timeout_ms, () => {
    perform_session_handshake(conn, self.socket_type, self.options)
  }) catch {
    err => {
      if @async.is_being_cancelled() {
        return
      }
      self.handshake_failures = self.handshake_failures + 1
      match err {
        AuthFailed(reason) => self.last_auth_error = Some(reason)
        ProtocolError(reason) => self.last_auth_error = Some(reason)
        _ => ()
      }
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
  self.send_initial_subscriptions(pipe)
  let _ = self.pipe_ready_notify.try_put(pipe_id) catch { _ => false }

  // Process any frames that arrived coalesced with the final handshake packet
  for item in hs.initial_items {
    if !self.handle_decoded_item(pipe, item) {
      self.detach_pipe(pipe_id)
      return
    }
  }

  // Spawn writer coroutine for this pipe
  let writer_task = group.spawn(
    no_wait=true,
    allow_failure=true,
    () => {
      while !self.closed && pipe.is_open() && !@async.is_being_cancelled() {
        match pipe.dequeue_outbound() {
          Some(wire) => {
            let _ = self.pipe_ready_notify.try_put(pipe_id) catch {
              _ => false
            }
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
    },
  )
  self.tasks.push(writer_task)

  // Run reader loop in current coroutine
  let decoder = hs.decoder
  while !self.closed && pipe.is_open() && !@async.is_being_cancelled() {
    let chunk = conn.read() catch { _ => None }
    if @async.is_being_cancelled() {
      break
    }
    match chunk {
      Some(b) => {
        if b.length() == 0 {
          break
        }
        let items = decoder.feed(b) catch {
          err => {
            let reason = match err {
              ProtocolError(m) => m
              FrameTooLarge(sz) => "frame size \{sz} exceeds limit"
              _ => "decode error"
            }
            conn.write(ZmtpCommand::ErrorCmd(reason).encode_frame()) catch {
              _ => ()
            }
            break
          }
        }
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
      None => break
    }
  }
  self.detach_pipe(pipe_id)
  writer_task.cancel()
}

///|
/// Dispatch handshake according to `SocketOptions`.
async fn perform_session_handshake(
  conn : @socket.Tcp,
  local_type : SocketType,
  options : SocketOptions,
) -> HandshakeResult raise ZmqError {
  if options.plain_server || options.plain_credentials is Some(_) {
    raise ProtocolError("PLAIN mechanism not configured")
  } else {
    perform_null_handshake(conn, local_type, options)
  }
}

///|
/// Bind a local `"tcp://host:port"` endpoint and start accepting peer connections
/// in the background within `group`. Returns the resolved bound endpoint
/// (e.g. `"tcp://127.0.0.1:54321"` when bound to port `0`).
pub async fn TransportHub::bind(
  self : TransportHub,
  group : @async.TaskGroup,
  endpoint : String,
) -> String raise ZmqError {
  if self.closed {
    raise Closed
  }
  let addr = parse_tcp_endpoint(endpoint, for_bind=true)
  let server = @socket.TcpServer(addr, reuse_addr=true) catch {
    err => raise IoError("failed to bind \{endpoint}: \{err}")
  }
  let bound_endpoint = "tcp://\{server.addr}"
  let accept_task = group.spawn(
    no_wait=true,
    allow_failure=true,
    () => {
      defer server.close()
      while !self.closed && !@async.is_being_cancelled() {
        let accepted = server.accept() catch { _ => break }
        let (conn, _peer_addr) = accepted
        if self.closed || @async.is_being_cancelled() {
          conn.close()
          break
        }
        let conn_task = group.spawn(
          no_wait=true,
          allow_failure=true,
          () => self.run_peer_connection(group, conn),
        )
        self.tasks.push(conn_task)
      }
    },
  )
  self.tasks.push(accept_task)
  bound_endpoint
}

///|
/// Connect to a remote `"tcp://host:port"` endpoint in the background within `group`.
/// Automatically reconnects on disconnect or failure using configurable initial
/// interval (`reconnect_ivl_ms`) and exponential backoff (`reconnect_ivl_max_ms`).
pub fn TransportHub::connect(
  self : TransportHub,
  group : @async.TaskGroup,
  endpoint : String,
) -> Unit raise ZmqError {
  if self.closed {
    raise Closed
  }
  let addr = parse_tcp_endpoint(endpoint, for_bind=false)
  let connect_task = group.spawn(
    no_wait=true,
    allow_failure=true,
    () => {
      let mut delay_ms = if self.options.reconnect_ivl_ms <= 0 {
        10
      } else {
        self.options.reconnect_ivl_ms
      }
      let mut first_attempt = true
      while !self.closed && !@async.is_being_cancelled() {
        if !first_attempt {
          self.reconnect_attempts = self.reconnect_attempts + 1
          @async.sleep(delay_ms)
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
        let conn_opt : @socket.Tcp? = Some(@socket.Tcp::connect(addr)) catch {
          _ => None
        }
        if @async.is_being_cancelled() {
          match conn_opt {
            Some(c) => c.close()
            None => ()
          }
          break
        }
        match conn_opt {
          Some(conn) => {
            let prev_peers = self.next_pipe_id
            self.run_peer_connection(group, conn)
            // If a pipe was successfully established during this run, reset backoff delay
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
    },
  )
  self.tasks.push(connect_task)
}

///|
/// Wait until at least one active peer pipe is connected and can accept outbound messages.
pub async fn TransportHub::wait_for_writable_pipe(
  self : TransportHub,
) -> Unit raise ZmqError {
  while !self.closed {
    for p in self.pipes {
      if p.can_send() {
        return
      }
    }
    let _ = self.pipe_ready_notify.get() catch { _ => raise Closed }
  }
  raise Closed
}

///|
/// Send a message using Round-Robin selection across all active peer pipes.
/// Used by `REQ`, `DEALER`, `PUSH`, and `PAIR`.
pub async fn TransportHub::send_round_robin(
  self : TransportHub,
  msg : Message,
  policy? : HwmPolicy = BlockOnFull,
) -> Unit raise ZmqError {
  while !self.closed {
    let n = self.pipes.length()
    if n > 0 {
      for offset = 0; offset < n; offset = offset + 1 {
        let idx = (self.rr_index + offset) % n
        let p = self.pipes[idx]
        if p.can_send() {
          self.rr_index = (idx + 1) % n
          let sent = p.send_message(msg, policy~)
          if sent {
            return
          }
        }
      }
      if policy == ErrorOnFull {
        raise HighWaterMarkReached
      }
      if policy == DropOnFull {
        return
      }
    } else if policy == ErrorOnFull {
      raise HighWaterMarkReached
    }
    let _ = self.pipe_ready_notify.get() catch { _ => raise Closed }
  }
  raise Closed
}

///|
/// Receive a message using Fair-Queueing across all active peer pipes.
/// Returns both the source `Pipe` and the `Message`.
pub async fn TransportHub::recv_fair_queue(
  self : TransportHub,
) -> (Pipe, Message) raise ZmqError {
  while !self.closed {
    let n = self.pipes.length()
    if n > 0 {
      for offset = 0; offset < n; offset = offset + 1 {
        let idx = (self.fq_index + offset) % n
        let p = self.pipes[idx]
        match p.try_recv_message() {
          Some(msg) => {
            self.fq_index = (idx + 1) % n
            return (p, msg)
          }
          None => ()
        }
      }
    }
    let _ = self.inbound_notify.get() catch { _ => raise Closed }
  }
  raise Closed
}

///|
/// Close all listeners, active connections, pipes, and background tasks.
pub fn TransportHub::close(self : TransportHub) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  self.inbound_notify.close(clear=true)
  self.pipe_ready_notify.close(clear=true)
  self.sub_events.close(clear=true)
  for p in self.pipes {
    p.close()
  }
  self.pipes.clear()
  for t in self.tasks {
    t.cancel()
  }
  self.tasks.clear()
}
