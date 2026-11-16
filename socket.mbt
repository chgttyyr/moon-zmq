///|
/// ZeroMQ `Context` managing the lifecycle of all created `Socket` instances.
pub struct Context {
  sockets : Array[Socket]
  mut closed : Bool
}

///|
/// Create a new ZeroMQ `Context`.
pub fn Context::new() -> Context {
  { sockets: [], closed: false, }
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
  let sock = Socket::new(socket_type, options~)
  self.sockets.push(sock)
  sock
}

///|
/// Terminate the `Context` and close all associated sockets.
pub fn Context::term(self : Context) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  for s in self.sockets {
    s.close()
  }
  self.sockets.clear()
}

///|
/// High-level ZeroMQ `Socket` implementing ZMTP 3.1 messaging patterns.
pub struct Socket {
  socket_type : SocketType
  options : SocketOptions
  hub : TransportHub
  priv mut req_state : ReqState
  priv mut rep_state : RepState
  mut closed : Bool
}

///|
/// Create a standalone `Socket` with the specified `SocketType` and `SocketOptions`.
pub fn Socket::new(
  socket_type : SocketType,
  options~ : SocketOptions = SocketOptions::new(),
) -> Socket {
  let hub = TransportHub::new(socket_type, options~)
  {
    socket_type,
    options,
    hub,
    req_state: SendReady,
    rep_state: RecvReady,
    closed: false,
  }
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
/// Bind a local `"tcp://host:port"` endpoint and begin accepting peers in `group`.
/// Returns the resolved endpoint string (e.g. `"tcp://127.0.0.1:54321"`).
pub async fn Socket::bind(
  self : Socket,
  group : @async.TaskGroup,
  endpoint : String,
) -> String raise ZmqError {
  if self.closed {
    raise Closed
  }
  self.hub.bind(group, endpoint)
}

///|
/// Connect to a remote `"tcp://host:port"` endpoint in `group` with automatic reconnection.
pub fn Socket::connect(
  self : Socket,
  group : @async.TaskGroup,
  endpoint : String,
) -> Unit raise ZmqError {
  if self.closed {
    raise Closed
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
        let event_opt = self.hub.sub_events.try_get() catch { _ => None }
        match event_opt {
          Some(msg) => return msg
          None => ()
        }
        let _ = self.hub.inbound_notify.get() catch { _ => raise Closed }
        let event_opt2 = self.hub.sub_events.try_get() catch { _ => None }
        match event_opt2 {
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
