///|
/// ZeroMQ `Context` managing the lifecycle of all created `Socket` instances
/// and providing an isolated `inproc://` transport namespace.
pub struct Context {
  sockets : Array[Socket]
  inproc : InprocRegistry
  mut closed : Bool
}

///|
/// Create a new ZeroMQ `Context`.
pub fn Context::new() -> Context {
  { sockets: [], inproc: InprocRegistry::new(), closed: false, }
}

///|
/// Return the `InprocRegistry` isolated within this `Context`.
pub fn Context::inproc_registry(self : Context) -> InprocRegistry {
  self.inproc
}

///|
/// Create a new `Socket` of the given `SocketType` within this `Context`.
pub fn Context::socket(
  self : Context,
  socket_type : SocketType,
  options~ : SocketOptions = SocketOptions::new(),
) -> Socket raise ZmqError {
  if self.closed {
    raise Closed
  }
  let sock = Socket::new_with_inproc(socket_type, self.inproc, options~)
  self.sockets.push(sock)
  sock
}

///|
/// Terminate the `Context` and close all associated sockets and `inproc://` endpoints.
pub fn Context::term(self : Context) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  self.inproc.close_all()
  for s in self.sockets {
    s.close()
  }
  self.sockets.clear()
}

///|
let next_socket_id : Ref[Int32] = { val: 1, }

///|
fn allocate_socket_id() -> Int32 {
  let id = next_socket_id.val
  next_socket_id.val = next_socket_id.val + 1
  id
}

///|
/// High-level ZeroMQ `Socket` implementing ZMTP 3.1 messaging patterns.
pub struct Socket {
  id : Int32
  socket_type : SocketType
  options : SocketOptions
  hub : TransportHub
  inproc_registry : InprocRegistry
  priv mut req_state : ReqState
  priv mut rep_state : RepState
  mut closed : Bool
}

///|
pub extend Socket with Eq::{not_equal, equal}

///|
pub impl Eq for Socket with fn equal(self, other) {
  self.id == other.id
}

///|
/// Create a standalone `Socket` with the specified `SocketType` and `SocketOptions`.
pub fn Socket::new(
  socket_type : SocketType,
  options~ : SocketOptions = SocketOptions::new(),
) -> Socket {
  Socket::new_with_inproc(socket_type, default_inproc_registry, options~)
}

///|
/// Create a `Socket` bound to a specific `InprocRegistry` namespace.
pub fn Socket::new_with_inproc(
  socket_type : SocketType,
  inproc_registry : InprocRegistry,
  options~ : SocketOptions = SocketOptions::new(),
) -> Socket {
  let hub = TransportHub::new(socket_type, options~)
  {
    id: allocate_socket_id(),
    socket_type,
    options,
    hub,
    inproc_registry,
    req_state: SendReady,
    rep_state: RecvReady,
    closed: false,
  }
}

///|
/// Unique numeric identifier of this `Socket`.
pub fn Socket::id(self : Socket) -> Int32 {
  self.id
}

///|
/// Whether this `Socket` has been closed.
pub fn Socket::is_closed(self : Socket) -> Bool {
  self.closed
}

///|
/// Whether this `Socket` can perform a non-blocking receive operation.
pub fn Socket::can_recv(self : Socket) -> Bool {
  if self.closed {
    return false
  }
  match self.socket_type {
    Push => false
    Req =>
      match self.req_state {
        WaitReply(p) => p.has_inbound()
        _ => false
      }
    Rep =>
      match self.rep_state {
        RecvReady => self.hub.can_recv()
        _ => false
      }
    XPub => self.hub.pending_sub_events > 0 || self.hub.can_recv()
    _ => self.hub.can_recv()
  }
}

///|
/// Whether this `Socket` can perform a non-blocking send operation.
pub fn Socket::can_send(self : Socket) -> Bool {
  if self.closed {
    return false
  }
  match self.socket_type {
    Pull => false
    Req =>
      match self.req_state {
        SendReady => self.hub.can_send()
        _ => false
      }
    Rep =>
      match self.rep_state {
        SendReply(_, _) => self.hub.can_send()
        _ => false
      }
    _ => self.hub.can_send()
  }
}

///|
/// Evaluate which events among requested `events` (`POLLIN`, `POLLOUT`, `POLLERR`) are currently ready.
pub fn Socket::has_events(self : Socket, events : Int32) -> Int32 {
  if self.closed {
    return if (events & 0x04) != 0 { 0x04 } else { 0 }
  }
  let mut revents : Int32 = 0
  if (events & 0x01) != 0 && self.can_recv() {
    revents = revents | 0x01
  }
  if (events & 0x02) != 0 && self.can_send() {
    revents = revents | 0x02
  }
  revents
}

///|
/// Set the ZMTP `Identity` (`Routing-Id`) of this socket before binding or connecting.
pub fn Socket::set_identity(self : Socket, identity : Bytes) -> Unit {
  self.options.set_identity(Some(identity))
}

///|
/// Set the outbound and inbound High-Water Marks (`sndhwm`, `rcvhwm`).
pub fn Socket::set_hwm(
  self : Socket,
  sndhwm~ : Int32 = DEFAULT_HWM,
  rcvhwm~ : Int32 = DEFAULT_HWM,
) -> Unit {
  self.options.set_sndhwm(sndhwm)
  self.options.set_rcvhwm(rcvhwm)
}

///|
/// Configure automatic reconnect initial interval and maximum exponential backoff in milliseconds.
pub fn Socket::set_reconnect_interval(
  self : Socket,
  ivl_ms : Int32,
  max_ms~ : Int32 = 5000,
) -> Unit {
  self.options.set_reconnect_interval(ivl_ms, max_ms~)
}

///|
/// Configure ZMTP 3.1 heartbeat ping-pong interval and activity timeout in milliseconds.
pub fn Socket::set_heartbeat(
  self : Socket,
  ivl_ms~ : Int32,
  timeout_ms~ : Int32 = 0,
  ttl_ms~ : Int32 = 0,
) -> Unit {
  self.options.set_heartbeat(ivl_ms~, timeout_ms~, ttl_ms~)
}

///|
/// Configure `ZMQ_ROUTER_MANDATORY` on a `ROUTER` socket.
pub fn Socket::set_router_mandatory(self : Socket, mandatory : Bool) -> Unit {
  self.options.set_router_mandatory(mandatory)
}

///|
/// Configure `ZMQ_XPUB_VERBOSE` on an `XPUB` socket.
pub fn Socket::set_xpub_verbose(self : Socket, verbose : Bool) -> Unit {
  self.options.set_xpub_verbose(verbose)
}

///|
/// Configure client-side `PLAIN` authentication credentials (`username`, `password`).
pub fn Socket::set_plain_credentials(
  self : Socket,
  username : String,
  password : String,
) -> Unit {
  self.options.set_plain_credentials(username, password)
}

///|
/// Configure server-side `PLAIN` authentication and optional ZAP verification callback.
pub fn Socket::set_plain_server(
  self : Socket,
  enabled : Bool,
  authenticator~ : ((String, String) -> Bool)? = None,
) -> Unit {
  self.options.set_plain_server(enabled, authenticator~)
}

///|
/// Bind a local `"tcp://host:port"` or `"inproc://name"` endpoint and begin accepting peers in `group`.
/// Returns the resolved endpoint string (e.g. `"tcp://127.0.0.1:54321"` or `"inproc://workers"`).
pub async fn Socket::bind(
  self : Socket,
  group : @async.TaskGroup,
  endpoint : String,
) -> String raise ZmqError {
  if self.closed {
    raise Closed
  }
  if endpoint.has_prefix("inproc://") {
    return self.hub.bind_inproc(group, endpoint, registry=self.inproc_registry)
  }
  self.hub.bind(group, endpoint)
}

///|
/// Connect to a remote `"tcp://host:port"` or `"inproc://name"` endpoint in `group`
/// with automatic reconnection and early-connect support.
pub fn Socket::connect(
  self : Socket,
  group : @async.TaskGroup,
  endpoint : String,
) -> Unit raise ZmqError {
  if self.closed {
    raise Closed
  }
  if endpoint.has_prefix("inproc://") {
    self.hub.connect_inproc(group, endpoint, registry=self.inproc_registry)
    return
  }
  self.hub.connect(group, endpoint)
}

///|
/// Number of currently active peer connections on this socket.
pub fn Socket::active_peer_count(self : Socket) -> Int32 {
  self.hub.active_peer_count()
}

///|
/// Send a `Message` according to this socket's pattern semantics.
pub async fn Socket::send(
  self : Socket,
  msg : Message,
  policy~ : HwmPolicy = BlockOnFull,
) -> Unit raise ZmqError {
  if self.closed {
    raise Closed
  }
  self.hub.record_send(msg)
  match self.socket_type {
    Req =>
      match self.req_state {
        SendReady => {
          let wrapped = wrap_req_envelope(msg)
          let pipe = self.hub.send_round_robin_with_pipe(wrapped, policy~)
          self.req_state = WaitReply(pipe)
        }
        WaitReply(_) =>
          raise InvalidState(
            "REQ socket cannot send while waiting for a reply (strict send/recv alternation)",
          )
      }
    Rep =>
      match self.rep_state {
        RecvReady =>
          raise InvalidState(
            "REP socket cannot send before receiving a request (strict recv/send alternation)",
          )
        SendReply(pipe, envelope) => {
          let wrapped = attach_rep_envelope(envelope, msg)
          self.rep_state = RecvReady
          if pipe.is_open() {
            let _ = pipe.send_message(wrapped, policy~) catch { _ => false }
          }
        }
      }
    Dealer | Push | Pair => self.hub.send_round_robin(msg, policy~)
    Router => self.hub.send_router(msg, policy~)
    Pub | XPub =>
      if self.socket_type == Pub {
        self.hub.send_pub(msg) catch {
          _ => ()
        }
      } else {
        self.hub.send_xsub(msg) catch {
          _ => ()
        }
      }
    XSub => self.hub.send_xsub(msg) catch { _ => () }
    Pull =>
      raise InvalidState(
        "PULL socket cannot send (receive-only socket per RFC 30)",
      )
    Sub =>
      raise InvalidState(
        "SUB socket cannot send application messages per RFC 29",
      )
  }
}

///|
/// Convenience helper to send a single-frame UTF-8/ASCII string message.
pub async fn Socket::send_string(
  self : Socket,
  s : String,
  policy~ : HwmPolicy = BlockOnFull,
) -> Unit raise ZmqError {
  self.send(Message::from_string(s), policy~)
}

///|
/// Convenience helper to send a single-frame binary message.
pub async fn Socket::send_bytes(
  self : Socket,
  b : Bytes,
  policy~ : HwmPolicy = BlockOnFull,
) -> Unit raise ZmqError {
  self.send(Message::from_bytes(b), policy~)
}

///|
/// Receive a `Message` according to this socket's pattern semantics.
pub async fn Socket::recv(self : Socket) -> Message raise ZmqError {
  let msg = self.recv_internal()
  self.hub.record_recv(msg)
  msg
}

///|
async fn Socket::recv_internal(self : Socket) -> Message raise ZmqError {
  if self.closed {
    raise Closed
  }
  match self.socket_type {
    Req =>
      match self.req_state {
        SendReady =>
          raise InvalidState(
            "REQ socket cannot recv before sending a request (strict send/recv alternation)",
          )
        WaitReply(pipe) => {
          while !self.closed {
            let raw = pipe.recv_message()
            match unwrap_req_envelope(raw) {
              Some(payload) => {
                self.req_state = SendReady
                return payload
              }
              None => ()
            }
          }
          raise Closed
        }
      }
    Rep =>
      match self.rep_state {
        SendReply(_, _) =>
          raise InvalidState(
            "REP socket cannot recv while a reply is pending (strict recv/send alternation)",
          )
        RecvReady => {
          while !self.closed {
            let (pipe, raw) = self.hub.recv_fair_queue()
            match split_rep_envelope(raw) {
              Some((envelope, payload)) => {
                self.rep_state = SendReply(pipe, envelope)
                return payload
              }
              None => ()
            }
          }
          raise Closed
        }
      }
    Pair | Dealer | Pull | Sub | XSub => {
      let (_, msg) = self.hub.recv_fair_queue()
      msg
    }
    Router => self.hub.recv_router()
    XPub => {
      while !self.closed {
        match self.hub.dequeue_sub_event() {
          Some(msg) => return msg
          None => ()
        }
        let _ = self.hub.inbound_notify.get() catch { _ => raise Closed }
        match self.hub.dequeue_sub_event() {
          Some(ev) => return ev
          None => {
            let n = self.hub.pipes.length()
            if n > 0 {
              for offset = 0; offset < n; offset = offset + 1 {
                let idx = (self.hub.fq_index + offset) % n
                let p = self.hub.pipes[idx]
                match p.try_recv_message() {
                  Some(msg) => {
                    self.hub.fq_index = (idx + 1) % n
                    return msg
                  }
                  None => ()
                }
              }
            }
          }
        }
      }
      raise Closed
    }
    Push =>
      raise InvalidState(
        "PUSH socket cannot recv (send-only socket per RFC 30)",
      )
    Pub =>
      raise InvalidState(
        "PUB socket cannot recv application messages per RFC 29",
      )
  }
}

///|
/// Convenience helper to receive a message and return its first frame as a `String`.
pub async fn Socket::recv_string(self : Socket) -> String raise ZmqError {
  let msg = self.recv()
  msg.first_string()
}

///|
/// Close the socket and release all underlying transport resources.
pub fn Socket::close(self : Socket) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  self.hub.close()
}

///|
/// Send an array of messages sequentially in a single batch operation.
pub async fn Socket::send_batch(
  self : Socket,
  messages : Array[Message],
  policy~ : HwmPolicy = BlockOnFull,
) -> Int32 raise ZmqError {
  if self.closed {
    raise Closed
  }
  let mut count = 0
  for msg in messages {
    self.send(msg, policy~)
    count = count + 1
  }
  count
}

///|
/// Receive up to `max_count` available messages in a single batch operation.
/// Asynchronously waits for at least the first message, then non-blockingly drains any ready messages.
pub async fn Socket::recv_batch(
  self : Socket,
  max_count~ : Int32 = 100,
) -> Array[Message] raise ZmqError {
  if self.closed {
    raise Closed
  }
  if max_count <= 0 {
    return []
  }
  let batch : Array[Message] = []
  let first = self.recv()
  batch.push(first)
  while batch.length() < max_count && self.can_recv() {
    let next_msg = self.recv()
    batch.push(next_msg)
  }
  batch
}

///|
/// Register a callback to receive `SocketEvent` lifecycle notifications.
pub fn Socket::on_event(self : Socket, handler : (SocketEvent) -> Unit) -> Unit {
  self.options.on_event(handler)
}

///|
/// Create an asynchronous queue that receives all `SocketEvent` occurrences for this socket.
pub fn Socket::monitor_events(
  self : Socket,
  capacity~ : Int32 = 100,
) -> @aqueue.Queue[SocketEvent] {
  let q : @aqueue.Queue[SocketEvent] = @aqueue.Queue(kind=Blocking(capacity))
  self.on_event(fn(ev) { let _ = q.try_put(ev) catch { _ => false } })
  q
}

///|
/// Total number of messages sent by this socket.
pub fn Socket::messages_sent(self : Socket) -> Int64 {
  self.hub.messages_sent()
}

///|
/// Total number of messages received by this socket.
pub fn Socket::messages_received(self : Socket) -> Int64 {
  self.hub.messages_received()
}

///|
/// Total payload bytes sent by this socket.
pub fn Socket::bytes_sent(self : Socket) -> Int64 {
  self.hub.bytes_sent()
}

///|
/// Total payload bytes received by this socket.
pub fn Socket::bytes_received(self : Socket) -> Int64 {
  self.hub.bytes_received()
}

///|
/// List of IDs of all currently active peer connections.
pub fn Socket::active_peer_ids(self : Socket) -> Array[Int32] {
  self.hub.active_peer_ids()
}
