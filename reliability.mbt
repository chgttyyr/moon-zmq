///|
/// ZeroMQ Reliability & Draft Messaging Patterns:
/// 1. Lazy Pirate / Multi-Endpoint Failover Reliable Client (`ReliableClient`)
/// 2. RFC 7 Majordomo Protocol (`MDP/0.2`) Service-Oriented Broker & RFC 8 `MMI`
/// 3. ZeroMQ Draft `Radio` / `Dish` Exact-Group Publish-Subscribe Pattern

///|
/// Canonical Majordomo Client Protocol v0.2 header (ZeroMQ RFC 7).
pub const MDP_CLIENT_HEADER : String = "MDPC02"

///|
/// Canonical Majordomo Worker Protocol v0.2 header (ZeroMQ RFC 7).
pub const MDP_WORKER_HEADER : String = "MDPW02"

///|
/// Command tags for Majordomo Worker Protocol (`MDP/0.2`, RFC 7 Section 2.2).
pub(all) enum MdpWorkerCommand {
  /// `0x01` — Worker signals readiness for a named service.
  Ready
  /// `0x02` — Broker dispatches a client request to a worker.
  Request
  /// `0x03` — Worker returns a service reply to the broker.
  Reply
  /// `0x04` — Bidirectional liveness heartbeat between broker and worker.
  Heartbeat
  /// `0x05` — Graceful disconnect notification.
  Disconnect
} derive(Eq, Debug)

///|
pub extend MdpWorkerCommand with Eq::{not_equal, equal}

///|
pub extend MdpWorkerCommand with @debug.Debug::{to_repr}

///|
/// Convert an `MdpWorkerCommand` to its single-byte RFC 7 wire tag.
pub fn MdpWorkerCommand::to_byte(self : MdpWorkerCommand) -> Byte {
  match self {
    Ready => b'\x01'
    Request => b'\x02'
    Reply => b'\x03'
    Heartbeat => b'\x04'
    Disconnect => b'\x05'
  }
}

///|
/// Parse a single-byte RFC 7 wire tag into `MdpWorkerCommand`.
pub fn MdpWorkerCommand::from_byte(b : Byte) -> MdpWorkerCommand? {
  match b.to_int() & 0xFF {
    1 => Some(Ready)
    2 => Some(Request)
    3 => Some(Reply)
    4 => Some(Heartbeat)
    5 => Some(Disconnect)
    _ => None
  }
}

///|
/// Encode an `MDPC02` client request addressed to `service` (`[b"", "MDPC02", service, ...body]`
/// when sent over `DEALER` to a `ROUTER` Majordomo broker).
pub fn encode_mdp_client_request(
  service : String,
  body : Array[Bytes],
) -> Message raise ZmqError {
  if service.is_empty() {
    raise ProtocolError("MDP/0.2 service name cannot be empty")
  }
  let frames : Array[Bytes] = Array(capacity=3 + body.length())
  frames.push(b"")
  frames.push(string_to_ascii_bytes(MDP_CLIENT_HEADER))
  frames.push(string_to_ascii_bytes(service))
  for f in body {
    frames.push(f)
  }
  Message::multipart(frames)
}

///|
/// Decode an `MDPC02` reply received on a `DEALER` client socket (`[b"", "MDPC02", service, ...body]`),
/// returning `(service_name, reply_frames)`.
pub fn decode_mdp_client_reply(
  msg : Message,
) -> (String, Array[Bytes]) raise ZmqError {
  if msg.frames.length() < 3 {
    raise ProtocolError("MDP/0.2 client reply requires at least 3 frames")
  }
  let offset = if msg.frames[0].length() == 0 { 1 } else { 0 }
  if msg.frames.length() < offset + 2 {
    raise ProtocolError("MDP/0.2 client reply truncated after empty delimiter")
  }
  let header = ascii_bytes_to_string(msg.frames[offset])
  if header != MDP_CLIENT_HEADER {
    raise ProtocolError("invalid MDP client header: \{header}")
  }
  let service = ascii_bytes_to_string(msg.frames[offset + 1])
  let body : Array[Bytes] = Array(capacity=msg.frames.length() - offset - 2)
  for i = offset + 2; i < msg.frames.length(); i = i + 1 {
    body.push(msg.frames[i])
  }
  (service, body)
}

///|
/// Encode an `MDPW02` worker `READY` packet (`[b"", "MDPW02", 0x01, service]`).
pub fn encode_mdp_worker_ready(service : String) -> Message raise ZmqError {
  if service.is_empty() {
    raise ProtocolError("MDP/0.2 worker service name cannot be empty")
  }
  Message::multipart([
    b"",
    string_to_ascii_bytes(MDP_WORKER_HEADER),
    Bytes::from_array([MdpWorkerCommand::Ready.to_byte()]),
    string_to_ascii_bytes(service),
  ])
}

///|
/// Encode an `MDPW02` worker `REPLY` packet (`[b"", "MDPW02", 0x03, client_id, b"", ...body]`).
pub fn encode_mdp_worker_reply(
  client_id : Bytes,
  body : Array[Bytes],
) -> Message {
  let frames : Array[Bytes] = Array(capacity=5 + body.length())
  frames.push(b"")
  frames.push(string_to_ascii_bytes(MDP_WORKER_HEADER))
  frames.push(Bytes::from_array([MdpWorkerCommand::Reply.to_byte()]))
  frames.push(client_id)
  frames.push(b"")
  for f in body {
    frames.push(f)
  }
  Message::multipart(frames)
}

///|
/// Encode an `MDPW02` `HEARTBEAT` or `DISCONNECT` control packet.
pub fn encode_mdp_worker_control(cmd : MdpWorkerCommand) -> Message {
  Message::multipart([
    b"",
    string_to_ascii_bytes(MDP_WORKER_HEADER),
    Bytes::from_array([cmd.to_byte()]),
  ])
}

///|
///Registered worker state inside a Majordomo `MdpBroker`.
priv struct MdpWorkerSlot {
  routing_id : Bytes
  id_hex : String
  service_name : String
  mut expires_at_ms : Int64
}

///|
/// Queued client request awaiting an available service worker inside `MdpBroker`.
priv struct MdpQueuedRequest {
  client_id : Bytes
  body : Array[Bytes]
}

///|
/// Service bucket grouping waiting requests and idle LRU workers for one service name.
priv struct MdpServiceQueue {
  name : String
  requests : Array[MdpQueuedRequest]
  idle_workers : Array[MdpWorkerSlot]
  mut known_workers : Int32
  mut dispatched_total : Int64
}

///|
/// ZeroMQ RFC 7 Majordomo (`MDP/0.2`) & RFC 8 (`MMI`) Service-Oriented Broker state machine.
///
/// Manages worker registration, Least-Recently-Used (LRU) dispatch, request queueing when
/// all workers are busy, worker liveness expiry, and built-in `"mmi.service"` discovery.
pub struct MdpBroker {
  heartbeat_liveness_ms : Int64
  priv services : Array[MdpServiceQueue]
  priv active_workers : Array[MdpWorkerSlot]
  mut total_client_requests : Int64
  mut total_worker_replies : Int64
  mut total_mmi_queries : Int64
}

///|
/// Create a new `MdpBroker` with configurable worker `heartbeat_liveness_ms`.
pub fn MdpBroker::new(heartbeat_liveness_ms~ : Int64 = 7500L) -> MdpBroker {
  let liveness = if heartbeat_liveness_ms <= 0L {
    2500L
  } else {
    heartbeat_liveness_ms
  }
  {
    heartbeat_liveness_ms: liveness,
    services: [],
    active_workers: [],
    total_client_requests: 0L,
    total_worker_replies: 0L,
    total_mmi_queries: 0L,
  }
}

///|
/// Number of registered workers (both idle and busy) currently tracked by the broker.
pub fn MdpBroker::worker_count(self : MdpBroker) -> Int32 {
  self.active_workers.length()
}

///|
/// Number of distinct service queues created on the broker.
pub fn MdpBroker::service_count(self : MdpBroker) -> Int32 {
  self.services.length()
}

///|
fn MdpBroker::get_or_create_service(
  self : MdpBroker,
  name : String,
) -> MdpServiceQueue {
  for svc in self.services {
    if svc.name == name {
      return svc
    }
  }
  let created : MdpServiceQueue = {
    name,
    requests: [],
    idle_workers: [],
    known_workers: 0,
    dispatched_total: 0L,
  }
  self.services.push(created)
  created
}

///|
fn MdpBroker::find_worker(self : MdpBroker, id_hex : String) -> MdpWorkerSlot? {
  for w in self.active_workers {
    if w.id_hex == id_hex {
      return Some(w)
    }
  }
  None
}

///|
fn MdpBroker::delete_worker(self : MdpBroker, id_hex : String) -> Unit {
  let mut svc_name = ""
  for i = 0; i < self.active_workers.length(); i = i + 1 {
    if self.active_workers[i].id_hex == id_hex {
      let removed = self.active_workers.remove(i)
      svc_name = removed.service_name
      break
    }
  }
  if !svc_name.is_empty() {
    for svc in self.services {
      if svc.name == svc_name {
        if svc.known_workers > 0 {
          svc.known_workers = svc.known_workers - 1
        }
        let mut j = 0
        while j < svc.idle_workers.length() {
          if svc.idle_workers[j].id_hex == id_hex {
            let _ = svc.idle_workers.remove(j)
          } else {
            j = j + 1
          }
        }
        break
      }
    }
  }
}

///|
/// Purge workers whose `expires_at_ms` has elapsed as of `now_ms`, returning
/// optional `DISCONNECT` notifications to send over the `ROUTER` socket.
pub fn MdpBroker::purge_expired_workers(
  self : MdpBroker,
  now_ms : Int64,
) -> Array[Message] {
  let expired_ids : Array[(String, Bytes)] = []
  for w in self.active_workers {
    if now_ms > w.expires_at_ms {
      expired_ids.push((w.id_hex, w.routing_id))
    }
  }
  let disconnects : Array[Message] = []
  for item in expired_ids {
    let (id_hex, routing_id) = item
    self.delete_worker(id_hex)
    disconnects.push(
      Message::multipart([
        routing_id,
        b"",
        string_to_ascii_bytes(MDP_WORKER_HEADER),
        Bytes::from_array([MdpWorkerCommand::Disconnect.to_byte()]),
      ]),
    )
  }
  disconnects
}

///|
/// Build `HEARTBEAT` router messages for all currently registered workers.
pub fn MdpBroker::build_worker_heartbeats(self : MdpBroker) -> Array[Message] {
  let out : Array[Message] = Array(capacity=self.active_workers.length())
  for w in self.active_workers {
    out.push(
      Message::multipart([
        w.routing_id,
        b"",
        string_to_ascii_bytes(MDP_WORKER_HEADER),
        Bytes::from_array([MdpWorkerCommand::Heartbeat.to_byte()]),
      ]),
    )
  }
  out
}

///|
fn MdpBroker::dispatch_service(
  self : MdpBroker,
  svc : MdpServiceQueue,
  now_ms : Int64,
  outbound : Array[Message],
) -> Unit {
  let _ = self.purge_expired_workers(now_ms)
  while !svc.idle_workers.is_empty() && !svc.requests.is_empty() {
    let worker = svc.idle_workers.remove(0)
    let req = svc.requests.remove(0)
    svc.dispatched_total = svc.dispatched_total + 1L
    let frames : Array[Bytes] = Array(capacity=6 + req.body.length())
    frames.push(worker.routing_id)
    frames.push(b"")
    frames.push(string_to_ascii_bytes(MDP_WORKER_HEADER))
    frames.push(Bytes::from_array([MdpWorkerCommand::Request.to_byte()]))
    frames.push(req.client_id)
    frames.push(b"")
    for f in req.body {
      frames.push(f)
    }
    outbound.push(Message::multipart(frames))
  }
}

///|
fn MdpBroker::handle_mmi_query(
  self : MdpBroker,
  client_id : Bytes,
  mmi_service : String,
  body : Array[Bytes],
) -> Message {
  self.total_mmi_queries = self.total_mmi_queries + 1L
  let code = if mmi_service == "mmi.service" {
    if body.is_empty() {
      "400"
    } else {
      let target_svc = ascii_bytes_to_string(body[0])
      let mut has_workers = false
      for svc in self.services {
        if svc.name == target_svc && svc.known_workers > 0 {
          has_workers = true
          break
        }
      }
      if has_workers {
        "200"
      } else {
        "404"
      }
    }
  } else {
    "501"
  }
  Message::multipart([
    client_id,
    b"",
    string_to_ascii_bytes(MDP_CLIENT_HEADER),
    string_to_ascii_bytes(mmi_service),
    string_to_ascii_bytes(code),
  ])
}

///|
/// Process a single inbound `ROUTER` message (either `MDPC02` from a client or
/// `MDPW02` from a worker) at `now_ms`, returning zero or more outbound `ROUTER` messages.
pub fn MdpBroker::handle_router_message(
  self : MdpBroker,
  raw : Message,
  now_ms~ : Int64 = 0L,
) -> Array[Message] raise ZmqError {
  if raw.frames.length() < 3 {
    raise ProtocolError("MDP/0.2 router frame requires at least 3 frames")
  }
  let sender_id = raw.frames[0]
  let idx = if raw.frames[1].length() == 0 { 2 } else { 1 }
  if idx >= raw.frames.length() {
    raise ProtocolError("MDP/0.2 router frame missing protocol header")
  }
  let header = ascii_bytes_to_string(raw.frames[idx])
  let outbound : Array[Message] = []
  if header == MDP_CLIENT_HEADER {
    if idx + 1 >= raw.frames.length() {
      raise ProtocolError("MDPC02 request missing service name frame")
    }
    self.total_client_requests = self.total_client_requests + 1L
    let svc_name = ascii_bytes_to_string(raw.frames[idx + 1])
    let body : Array[Bytes] = Array(capacity=raw.frames.length() - idx - 2)
    for i = idx + 2; i < raw.frames.length(); i = i + 1 {
      body.push(raw.frames[i])
    }
    if svc_name.has_prefix("mmi.") {
      outbound.push(self.handle_mmi_query(sender_id, svc_name, body))
      return outbound
    }
    let svc = self.get_or_create_service(svc_name)
    svc.requests.push({ client_id: sender_id, body, })
    self.dispatch_service(svc, now_ms, outbound)
    return outbound
  }
  if header == MDP_WORKER_HEADER {
    if idx + 1 >= raw.frames.length() || raw.frames[idx + 1].length() != 1 {
      raise ProtocolError("MDPW02 message missing 1-byte command tag")
    }
    let cmd_opt = MdpWorkerCommand::from_byte(raw.frames[idx + 1][0])
    let cmd = match cmd_opt {
      Some(c) => c
      None => raise ProtocolError("unknown MDPW02 command tag")
    }
    let id_hex = bytes_to_hex(sender_id)
    let existing = self.find_worker(id_hex)
    match cmd {
      Ready => {
        if existing is Some(_) {
          self.delete_worker(id_hex)
        }
        if idx + 2 >= raw.frames.length() {
          raise ProtocolError("MDPW02 READY missing service name")
        }
        let svc_name = ascii_bytes_to_string(raw.frames[idx + 2])
        let slot : MdpWorkerSlot = {
          routing_id: sender_id,
          id_hex,
          service_name: svc_name,
          expires_at_ms: now_ms + self.heartbeat_liveness_ms,
        }
        self.active_workers.push(slot)
        let svc = self.get_or_create_service(svc_name)
        svc.known_workers = svc.known_workers + 1
        svc.idle_workers.push(slot)
        self.dispatch_service(svc, now_ms, outbound)
      }
      Reply =>
        match existing {
          Some(worker) => {
            if idx + 3 >= raw.frames.length() {
              raise ProtocolError("MDPW02 REPLY missing client envelope")
            }
            worker.expires_at_ms = now_ms + self.heartbeat_liveness_ms
            let client_id = raw.frames[idx + 2]
            let body_start = if raw.frames[idx + 3].length() == 0 {
              idx + 4
            } else {
              idx + 3
            }
            let reply_frames : Array[Bytes] = Array(
              capacity=4 + raw.frames.length() - body_start,
            )
            reply_frames.push(client_id)
            reply_frames.push(b"")
            reply_frames.push(string_to_ascii_bytes(MDP_CLIENT_HEADER))
            reply_frames.push(string_to_ascii_bytes(worker.service_name))
            for i = body_start; i < raw.frames.length(); i = i + 1 {
              reply_frames.push(raw.frames[i])
            }
            self.total_worker_replies = self.total_worker_replies + 1L
            outbound.push(Message::multipart(reply_frames))
            let svc = self.get_or_create_service(worker.service_name)
            svc.idle_workers.push(worker)
            self.dispatch_service(svc, now_ms, outbound)
          }
          None => {
            self.delete_worker(id_hex)
            outbound.push(
              Message::multipart([
                sender_id,
                b"",
                string_to_ascii_bytes(MDP_WORKER_HEADER),
                Bytes::from_array([MdpWorkerCommand::Disconnect.to_byte()]),
              ]),
            )
          }
        }
      Heartbeat =>
        match existing {
          Some(worker) =>
            worker.expires_at_ms = now_ms + self.heartbeat_liveness_ms
          None =>
            outbound.push(
              Message::multipart([
                sender_id,
                b"",
                string_to_ascii_bytes(MDP_WORKER_HEADER),
                Bytes::from_array([MdpWorkerCommand::Disconnect.to_byte()]),
              ]),
            )
        }
      Disconnect => self.delete_worker(id_hex)
      Request => raise ProtocolError("worker cannot send REQUEST to broker")
    }
    return outbound
  }
  raise ProtocolError("unrecognized Majordomo protocol header: \{header}")
}

///|
/// Reliable Request-Reply client implementing the ZeroMQ Guide Chapter 4
/// "Lazy Pirate" + multi-endpoint failover pattern.
///
/// Unlike raw `REQ` (which blocks forever if a single `REP` peer crashes mid-call),
/// `ReliableClient` enforces per-attempt `timeout_ms`, retries up to `max_retries`,
/// recreates the underlying socket on timeout to clear poisoned state, and rotates
/// across all configured `endpoints`.
pub struct ReliableClient {
  ctx : Context
  endpoints : Array[String]
  mut endpoint_index : Int32
  timeout_ms : Int32
  max_retries : Int32
  mut active_socket : Socket?
  mut requests_sent : Int64
  mut retries_count : Int64
  mut failovers_count : Int64
}

///|
/// Create a `ReliableClient` configured with one or more server `endpoints`,
/// per-try `timeout_ms`, and `max_retries`.
pub fn ReliableClient::new(
  ctx : Context,
  endpoints : Array[String],
  timeout_ms~ : Int32 = 1000,
  max_retries~ : Int32 = 3,
) -> ReliableClient raise ZmqError {
  if endpoints.is_empty() {
    raise InvalidEndpoint("ReliableClient requires at least one endpoint")
  }
  let eff_timeout = if timeout_ms <= 0 { 250 } else { timeout_ms }
  let eff_retries = if max_retries < 0 { 0 } else { max_retries }
  {
    ctx,
    endpoints,
    endpoint_index: 0,
    timeout_ms: eff_timeout,
    max_retries: eff_retries,
    active_socket: None,
    requests_sent: 0L,
    retries_count: 0L,
    failovers_count: 0L,
  }
}

///|
fn ReliableClient::ensure_socket(
  self : ReliableClient,
  group : @async.TaskGroup,
) -> Socket raise ZmqError {
  match self.active_socket {
    Some(sock) => if !sock.is_closed() { return sock }
    None => ()
  }
  let sock = self.ctx.socket(Req)
  let ep = self.endpoints[self.endpoint_index]
  sock.connect(group, ep)
  self.active_socket = Some(sock)
  sock
}

///|
fn ReliableClient::reset_and_rotate(self : ReliableClient) -> Unit {
  match self.active_socket {
    Some(sock) => sock.close()
    None => ()
  }
  self.active_socket = None
  if self.endpoints.length() > 1 {
    self.endpoint_index = (self.endpoint_index + 1) % self.endpoints.length()
    self.failovers_count = self.failovers_count + 1L
  }
}

///|
/// Send `request` and wait for a `REP` response within `timeout_ms`, automatically
/// retrying and rotating endpoints up to `max_retries` times before raising `IoError`.
pub async fn ReliableClient::request(
  self : ReliableClient,
  group : @async.TaskGroup,
  request : Message,
) -> Message raise ZmqError {
  let max_attempts = self.max_retries + 1
  let mut attempt = 0
  while attempt < max_attempts {
    if attempt > 0 {
      self.retries_count = self.retries_count + 1L
    }
    attempt = attempt + 1
    let sock = self.ensure_socket(group)
    // Wait up to timeout_ms for peer connection readiness
    let poller = Poller::new()
    poller.add(sock, POLLOUT)
    let writable = poller.poll(timeout_ms=self.timeout_ms) catch {
      _ => raise Closed
    }
    if writable <= 0 || !poller.has_out(sock) {
      self.reset_and_rotate()
      continue
    }
    self.requests_sent = self.requests_sent + 1L
    sock.send(request)
    let _ = poller.modify(sock, POLLIN)
    let readable = poller.poll(timeout_ms=self.timeout_ms) catch {
      _ => raise Closed
    }
    if readable > 0 && poller.has_in(sock) {
      return sock.recv()
    }
    // Peer stalled or dropped mid-request: tear down socket and rotate endpoint
    self.reset_and_rotate()
  }
  raise IoError(
    "ReliableClient exhausted \{max_attempts} attempts without reply",
  )
}

///|
/// Close the underlying socket held by this `ReliableClient`.
pub fn ReliableClient::close(self : ReliableClient) -> Unit {
  match self.active_socket {
    Some(sock) => sock.close()
    None => ()
  }
  self.active_socket = None
}

///|
/// Total number of retries triggered by timeouts on this `ReliableClient`.
pub fn ReliableClient::retries(self : ReliableClient) -> Int64 {
  self.retries_count
}

///|
/// Total number of endpoint failover rotations performed by this `ReliableClient`.
pub fn ReliableClient::failovers(self : ReliableClient) -> Int64 {
  self.failovers_count
}

///|
/// Build the exact-match subscription frame header `[b'G', b'R', len:u8, ...group]`
/// used by `Radio` / `Dish` so group `"alerts"` never prefix-matches `"alerts.eu"`.
pub fn encode_group_topic_header(group_name : String) -> Bytes raise ZmqError {
  let raw = string_to_ascii_bytes(group_name)
  if raw.is_empty() || raw.length() > 255 {
    let n = raw.length()
    raise ProtocolError(
      "Radio/Dish group name length must be 1..255 bytes, got \{n}",
    )
  }
  let out : Array[Byte] = Array(capacity=3 + raw.length())
  out.push(b'G')
  out.push(b'R')
  out.push(raw.length().to_byte())
  for i = 0; i < raw.length(); i = i + 1 {
    out.push(raw[i])
  }
  Bytes::from_array(out)
}

///|
/// Decode the exact-match group header from frame 0 of a `Radio` / `Dish` packet.
pub fn decode_group_topic_header(header : Bytes) -> String raise ZmqError {
  if header.length() < 4 || header[0] != b'G' || header[1] != b'R' {
    raise ProtocolError("malformed Radio/Dish group frame header")
  }
  let expected_len = header[2].to_int() & 0xFF
  if header.length() != 3 + expected_len {
    raise ProtocolError("Radio/Dish group header length mismatch")
  }
  ascii_bytes_to_string(slice_bytes(header, 3, header.length()))
}

///|
/// ZeroMQ Draft `RADIO` publisher broadcasting exact-group messages to `DISH` peers.
pub struct RadioSocket {
  socket : Socket
}

///|
/// Create a new `RadioSocket` inside `ctx` (or standalone via `RadioSocket::standalone`).
pub fn RadioSocket::new(
  ctx : Context,
  options~ : SocketOptions = SocketOptions::new(),
) -> RadioSocket raise ZmqError {
  { socket: ctx.socket(Pub, options~), }
}

///|
/// Create a standalone `RadioSocket` outside an explicit `Context`.
pub fn RadioSocket::standalone(
  options~ : SocketOptions = SocketOptions::new(),
) -> RadioSocket {
  { socket: Socket::new(Pub, options~), }
}

///|
/// Bind this `RadioSocket` to a `tcp://` or `inproc://` endpoint.
pub async fn RadioSocket::bind(
  self : RadioSocket,
  group : @async.TaskGroup,
  endpoint : String,
) -> String raise ZmqError {
  self.socket.bind(group, endpoint)
}

///|
/// Connect this `RadioSocket` to a `tcp://` or `inproc://` endpoint.
pub fn RadioSocket::connect(
  self : RadioSocket,
  group : @async.TaskGroup,
  endpoint : String,
) -> Unit raise ZmqError {
  self.socket.connect(group, endpoint)
}

///|
/// Publish a `Message` exclusively to `DishSocket` peers that have joined exact `group_name`.
pub async fn RadioSocket::send_group(
  self : RadioSocket,
  group_name : String,
  payload : Message,
) -> Unit raise ZmqError {
  let hdr = encode_group_topic_header(group_name)
  let frames : Array[Bytes] = Array(capacity=1 + payload.frames.length())
  frames.push(hdr)
  for f in payload.frames {
    frames.push(f)
  }
  self.socket.send(Message::multipart(frames))
}

///|
/// Convenience helper publishing a UTF-8/ASCII string message to exact `group_name`.
pub async fn RadioSocket::send_group_string(
  self : RadioSocket,
  group_name : String,
  text : String,
) -> Unit raise ZmqError {
  self.send_group(group_name, Message::from_string(text))
}

///|
/// Close the underlying publisher socket.
pub fn RadioSocket::close(self : RadioSocket) -> Unit {
  self.socket.close()
}

///|
/// ZeroMQ Draft `DISH` subscriber receiving messages only from explicitly joined groups.
pub struct DishSocket {
  socket : Socket
  priv joined : Array[String]
}

///|
/// Create a new `DishSocket` inside `ctx`.
pub fn DishSocket::new(
  ctx : Context,
  options~ : SocketOptions = SocketOptions::new(),
) -> DishSocket raise ZmqError {
  { socket: ctx.socket(Sub, options~), joined: [], }
}

///|
/// Create a standalone `DishSocket` outside an explicit `Context`.
pub fn DishSocket::standalone(
  options~ : SocketOptions = SocketOptions::new(),
) -> DishSocket {
  { socket: Socket::new(Sub, options~), joined: [], }
}

///|
/// Bind this `DishSocket` to a `tcp://` or `inproc://` endpoint.
pub async fn DishSocket::bind(
  self : DishSocket,
  group : @async.TaskGroup,
  endpoint : String,
) -> String raise ZmqError {
  self.socket.bind(group, endpoint)
}

///|
/// Connect this `DishSocket` to a `tcp://` or `inproc://` endpoint.
pub fn DishSocket::connect(
  self : DishSocket,
  group : @async.TaskGroup,
  endpoint : String,
) -> Unit raise ZmqError {
  self.socket.connect(group, endpoint)
}

///|
/// Join exact `group_name` (`1..255` characters). Idempotent if already joined.
pub async fn DishSocket::join(
  self : DishSocket,
  group_name : String,
) -> Unit raise ZmqError {
  let hdr = encode_group_topic_header(group_name)
  for g in self.joined {
    if g == group_name {
      return
    }
  }
  self.joined.push(group_name)
  self.socket.subscribe(hdr)
}

///|
/// Leave exact `group_name`. Returns `true` if previously joined.
pub async fn DishSocket::leave(
  self : DishSocket,
  group_name : String,
) -> Bool raise ZmqError {
  let hdr = encode_group_topic_header(group_name)
  let mut removed = false
  for i = 0; i < self.joined.length(); i = i + 1 {
    if self.joined[i] == group_name {
      let _ = self.joined.remove(i)
      removed = true
      break
    }
  }
  if removed {
    self.socket.unsubscribe(hdr)
  }
  removed
}

///|
/// Check whether this `DishSocket` is currently a member of `group_name`.
pub fn DishSocket::is_joined(self : DishSocket, group_name : String) -> Bool {
  for g in self.joined {
    if g == group_name {
      return true
    }
  }
  false
}

///|
/// Return a snapshot of all group names currently joined by this `DishSocket`.
pub fn DishSocket::groups(self : DishSocket) -> Array[String] {
  let copy : Array[String] = Array(capacity=self.joined.length())
  for g in self.joined {
    copy.push(g)
  }
  copy
}

///|
/// Receive the next group publication as `(group_name, payload_message)`.
pub async fn DishSocket::recv_group(
  self : DishSocket,
) -> (String, Message) raise ZmqError {
  while true {
    let raw = self.socket.recv()
    if raw.frames.is_empty() {
      continue
    }
    let group_name = decode_group_topic_header(raw.frames[0])
    if !self.is_joined(group_name) {
      continue
    }
    let payload_frames : Array[Bytes] = Array(capacity=raw.frames.length() - 1)
    for i = 1; i < raw.frames.length(); i = i + 1 {
      payload_frames.push(raw.frames[i])
    }
    return (group_name, Message::multipart(payload_frames))
  }
  raise Closed
}

///|
/// Close the underlying subscriber socket.
pub fn DishSocket::close(self : DishSocket) -> Unit {
  self.socket.close()
}
