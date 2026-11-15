///|
/// High-water mark (HWM) overflow behavior when a pipe's outbound queue is full.
pub(all) enum HwmPolicy {
  /// Block asynchronously until queue space becomes available.
  BlockOnFull
  /// Immediately drop the message silently (used by PUB/XPUB per RFC 29).
  DropOnFull
  /// Immediately raise `ZmqError::HighWaterMarkReached`.
  ErrorOnFull
} derive(Eq, Debug)

///|
pub extend HwmPolicy with Eq::{not_equal, equal}

///|
pub extend HwmPolicy with @debug.Debug::{to_repr}

///|
/// Bidirectional message pipe representing a single active ZMTP peer connection.
/// Enforces send/receive High-Water Marks (`sndhwm`, `rcvhwm`) and tracks per-peer
/// routing identity and PUB/XPUB topic subscriptions.
pub struct Pipe {
  id : Int
  peer_identity : Bytes
  peer_metadata : Metadata
  sndhwm : Int
  rcvhwm : Int
  snd_queue : @aqueue.Queue[Bytes]
  rcv_queue : @aqueue.Queue[Message]
  mut snd_count : Int
  mut rcv_count : Int
  subscriptions : Array[Bytes]
  mut closed : Bool
}

///|
/// Default send/receive high-water mark (1000 messages, matching libzmq default).
pub const DEFAULT_HWM : Int = 1000

///|
/// Generate a deterministic 5-byte routing identity (`\x00` + 4-byte big-endian id)
/// when a peer does not supply an explicit `Identity` property in its handshake metadata.
pub fn default_routing_id(pipe_id : Int) -> Bytes {
  Bytes::from_array([
    b'\x00',
    ((pipe_id >> 24) & 0xFF).to_byte(),
    ((pipe_id >> 16) & 0xFF).to_byte(),
    ((pipe_id >> 8) & 0xFF).to_byte(),
    (pipe_id & 0xFF).to_byte(),
  ])
}

///|
/// Create a new `Pipe` for an established peer session.
pub fn Pipe::new(
  id : Int,
  peer_metadata : Metadata,
  sndhwm? : Int = DEFAULT_HWM,
  rcvhwm? : Int = DEFAULT_HWM,
) -> Pipe {
  let effective_snd = if sndhwm <= 0 { 1 } else { sndhwm }
  let effective_rcv = if rcvhwm <= 0 { 1 } else { rcvhwm }
  let peer_identity = match peer_metadata.identity {
    Some(ident) => if ident.length() > 0 { ident } else { default_routing_id(id) }
    None => default_routing_id(id)
  }
  {
    id,
    peer_identity,
    peer_metadata,
    sndhwm: effective_snd,
    rcvhwm: effective_rcv,
    snd_queue: @aqueue.Queue(kind=Blocking(effective_snd)),
    rcv_queue: @aqueue.Queue(kind=Blocking(effective_rcv)),
    snd_count: 0,
    rcv_count: 0,
    subscriptions: [],
    closed: false,
  }
}

///|
/// Whether the pipe is still open.
pub fn Pipe::is_open(self : Pipe) -> Bool {
  !self.closed
}

///|
/// Whether the outbound queue is below the send high-water mark (`sndhwm`).
pub fn Pipe::can_send(self : Pipe) -> Bool {
  !self.closed && self.snd_count < self.sndhwm
}

///|
/// Whether the inbound queue currently has at least one message ready to read.
pub fn Pipe::has_inbound(self : Pipe) -> Bool {
  self.rcv_count > 0
}

///|
/// Enqueue raw wire bytes (an encoded `Message` or `ZmtpCommand`) for transmission
/// according to the specified `HwmPolicy`. Returns `true` if enqueued, `false` if dropped.
pub async fn Pipe::send_wire(
  self : Pipe,
  wire : Bytes,
  policy? : HwmPolicy = BlockOnFull,
) -> Bool raise ZmqError {
  if self.closed {
    raise Closed
  }
  match policy {
    DropOnFull => {
      if self.snd_count >= self.sndhwm {
        return false
      }
      let ok = self.snd_queue.try_put(wire) catch { _ => raise Closed }
      if ok {
        self.snd_count = self.snd_count + 1
      }
      ok
    }
    ErrorOnFull => {
      if self.snd_count >= self.sndhwm {
        raise HighWaterMarkReached
      }
      let ok = self.snd_queue.try_put(wire) catch { _ => raise Closed }
      if !ok {
        raise HighWaterMarkReached
      }
      self.snd_count = self.snd_count + 1
      true
    }
    BlockOnFull => {
      let ok = self.snd_queue.try_put(wire) catch { _ => raise Closed }
      if ok {
        self.snd_count = self.snd_count + 1
        return true
      }
      self.snd_queue.put(wire) catch {
        _ => raise Closed
      }
      self.snd_count = self.snd_count + 1
      true
    }
  }
}

///|
/// Non-blocking attempt to enqueue an encoded `Message` onto this pipe.
pub fn Pipe::try_send_message(
  self : Pipe,
  msg : Message,
) -> Bool raise ZmqError {
  if self.closed {
    raise Closed
  }
  if self.snd_count >= self.sndhwm {
    return false
  }
  let ok = self.snd_queue.try_put(msg.encode()) catch { _ => raise Closed }
  if ok {
    self.snd_count = self.snd_count + 1
  }
  ok
}

///|
/// Enqueue a `Message` onto this pipe using the given `HwmPolicy`.
pub async fn Pipe::send_message(
  self : Pipe,
  msg : Message,
  policy? : HwmPolicy = BlockOnFull,
) -> Bool raise ZmqError {
  self.send_wire(msg.encode(), policy~)
}

///|
/// Dequeue the next outbound wire frame from `snd_queue` (used by the TCP writer task).
pub async fn Pipe::dequeue_outbound(self : Pipe) -> Bytes? {
  let wire = self.snd_queue.get() catch { _ => return None }
  if self.snd_count > 0 {
    self.snd_count = self.snd_count - 1
  }
  Some(wire)
}

///|
/// Push a decoded inbound `Message` from the TCP reader task into `rcv_queue`.
/// Respects `rcvhwm`: if `drop_on_full` is true, drops when full; otherwise blocks.
pub async fn Pipe::push_inbound(
  self : Pipe,
  msg : Message,
  drop_on_full? : Bool = false,
) -> Bool {
  if self.closed {
    return false
  }
  if self.rcv_count < self.rcvhwm {
    let ok = self.rcv_queue.try_put(msg) catch { _ => false }
    if ok {
      self.rcv_count = self.rcv_count + 1
      return true
    }
  }
  if drop_on_full {
    return false
  }
  self.rcv_queue.put(msg) catch {
    _ => return false
  }
  self.rcv_count = self.rcv_count + 1
  true
}

///|
/// Non-blocking attempt to pop an inbound `Message` from this pipe.
pub fn Pipe::try_recv_message(self : Pipe) -> Message? {
  let res = self.rcv_queue.try_get() catch { _ => None }
  match res {
    Some(m) => {
      if self.rcv_count > 0 {
        self.rcv_count = self.rcv_count - 1
      }
      Some(m)
    }
    None => None
  }
}

///|
/// Asynchronously wait for and pop an inbound `Message` from this pipe.
pub async fn Pipe::recv_message(self : Pipe) -> Message raise ZmqError {
  let msg = self.rcv_queue.get() catch { _ => raise Closed }
  if self.rcv_count > 0 {
    self.rcv_count = self.rcv_count - 1
  }
  msg
}

///|
/// Add a topic subscription prefix to this pipe. Returns `true` if this prefix
/// was not already subscribed on this pipe.
pub fn Pipe::add_subscription(self : Pipe, topic : Bytes) -> Bool {
  for existing in self.subscriptions {
    if existing == topic {
      return false
    }
  }
  self.subscriptions.push(topic)
  true
}

///|
/// Remove a topic subscription prefix from this pipe. Returns `true` if removed.
pub fn Pipe::remove_subscription(self : Pipe, topic : Bytes) -> Bool {
  let mut idx = -1
  for i = 0; i < self.subscriptions.length(); i = i + 1 {
    if self.subscriptions[i] == topic {
      idx = i
      break
    }
  }
  if idx < 0 {
    return false
  }
  let _ = self.subscriptions.remove(idx)
  true
}

///|
/// Check if a message's first frame matches any subscription prefix registered on this pipe.
pub fn Pipe::matches_subscription(self : Pipe, msg : Message) -> Bool {
  let first = msg.first_bytes()
  for prefix in self.subscriptions {
    if bytes_starts_with(first, prefix) {
      return true
    }
  }
  false
}

///|
/// Close the pipe and both of its underlying queues.
pub fn Pipe::close(self : Pipe) -> Unit {
  if !self.closed {
    self.closed = true
    self.snd_queue.close(clear=true)
    self.rcv_queue.close(clear=true)
    self.snd_count = 0
    self.rcv_count = 0
  }
}
