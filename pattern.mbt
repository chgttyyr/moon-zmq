///|
/// State machine for a `REQ` socket (RFC 28 Section 2.1).
/// Strictly alternates between sending a request and waiting for a reply from the same peer.
priv enum ReqState {
  SendReady
  WaitReply(Pipe)
}

///|
/// State machine for a `REP` socket (RFC 28 Section 2.3).
/// Strictly alternates between receiving a request (saving the peer pipe and full
/// routing envelope stack up to and including the empty delimiter frame) and sending a reply.
priv enum RepState {
  RecvReady
  SendReply(Pipe, Array[Bytes])
}

///|
/// Prepend an empty delimiter frame (`b""`) to an application `Message`
/// as required by `REQ` when sending a request (RFC 28 Section 2.1).
pub fn wrap_req_envelope(msg : Message) -> Message {
  let frames : Array[Bytes] = Array(capacity=msg.frames.length() + 1)
  frames.push(b"")
  for f in msg.frames {
    frames.push(f)
  }
  { frames, }
}

///|
/// Strip the leading empty delimiter frame (`b""`) from an inbound `REQ` reply.
/// Returns `Some(payload_msg)` if the first frame is empty and at least one payload
/// frame follows, or `None` if malformed per RFC 28 Section 2.1.
pub fn unwrap_req_envelope(raw : Message) -> Message? {
  if raw.frames.length() < 2 {
    return None
  }
  if raw.frames[0].length() != 0 {
    return None
  }
  let frames : Array[Bytes] = Array(capacity=raw.frames.length() - 1)
  for i = 1; i < raw.frames.length(); i = i + 1 {
    frames.push(raw.frames[i])
  }
  Some({ frames, })
}

///|
/// Split an inbound `REP` message into `(envelope_stack, payload_message)` at the
/// first empty delimiter frame (`b""`), preserving any upstream routing frames
/// prepended by `ROUTER` intermediaries (RFC 28 Section 2.3).
pub fn split_rep_envelope(raw : Message) -> (Array[Bytes], Message)? {
  let mut delim_idx = -1
  for i = 0; i < raw.frames.length(); i = i + 1 {
    if raw.frames[i].length() == 0 {
      delim_idx = i
      break
    }
  }
  if delim_idx < 0 || delim_idx >= raw.frames.length() - 1 {
    return None
  }
  let envelope : Array[Bytes] = Array(capacity=delim_idx + 1)
  for i = 0; i <= delim_idx; i = i + 1 {
    envelope.push(raw.frames[i])
  }
  let payload : Array[Bytes] = Array(
    capacity=raw.frames.length() - delim_idx - 1,
  )
  for i = delim_idx + 1; i < raw.frames.length(); i = i + 1 {
    payload.push(raw.frames[i])
  }
  Some((envelope, { frames: payload }))
}

///|
/// Re-attach a saved `REP` routing envelope stack (including the empty delimiter)
/// to an application reply `Message`.
pub fn attach_rep_envelope(envelope : Array[Bytes], reply : Message) -> Message {
  let frames : Array[Bytes] = Array(
    capacity=envelope.length() + reply.frames.length(),
  )
  for f in envelope {
    frames.push(f)
  }
  for f in reply.frames {
    frames.push(f)
  }
  { frames, }
}

///|
/// Select an active writable `Pipe` in Round-Robin order, send `msg`, and return
/// the selected `Pipe` (used by `REQ` to correlate replies from the same peer).
async fn TransportHub::send_round_robin_with_pipe(
  self : TransportHub,
  msg : Message,
  policy? : HwmPolicy = BlockOnFull,
) -> Pipe raise ZmqError {
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
            return p
          }
        }
      }
      if policy == ErrorOnFull {
        raise HighWaterMarkReached
      }
    } else if policy == ErrorOnFull {
      raise HighWaterMarkReached
    }
    let _ = self.pipe_ready_notify.get() catch { _ => raise Closed }
  }
  raise Closed
}

///|
/// Send a message from a `ROUTER` socket using the first frame as the destination
/// `peer_identity` (RFC 28 Section 2.2).
///
/// Strips the routing ID frame and routes the remaining message frames to the
/// matching peer `Pipe`. If no matching pipe exists or if the pipe queue is full:
/// - If `router_mandatory` is true, raises `Unroutable(target_id)` or `HighWaterMarkReached`.
/// - Otherwise, silently drops the message per RFC 28 default behavior.
async fn TransportHub::send_router(
  self : TransportHub,
  msg : Message,
  policy? : HwmPolicy = BlockOnFull,
) -> Unit raise ZmqError {
  if msg.frames.length() < 2 {
    if self.options.router_mandatory {
      raise ProtocolError("ROUTER outgoing message must contain at least identity + 1 frame")
    }
    return
  }
  let target_id = msg.frames[0]
  let payload_frames : Array[Bytes] = Array(capacity=msg.frames.length() - 1)
  for i = 1; i < msg.frames.length(); i = i + 1 {
    payload_frames.push(msg.frames[i])
  }
  let payload_msg = Message::multipart(payload_frames)

  // Find destination pipe
  let mut target_pipe : Pipe? = None
  for p in self.pipes {
    if p.peer_identity == target_id && p.is_open() {
      target_pipe = Some(p)
      break
    }
  }

  match target_pipe {
    Some(pipe) => {
      let sent = pipe.send_message(payload_msg, policy~) catch {
        HighWaterMarkReached =>
          if self.options.router_mandatory {
            raise HighWaterMarkReached
          } else {
            return
          }
        err => raise err
      }
      if !sent && self.options.router_mandatory {
        raise HighWaterMarkReached
      }
    }
    None => {
      if self.options.router_mandatory {
        raise Unroutable(target_id)
      }
    }
  }
}

///|
/// Receive a message on a `ROUTER` socket (RFC 28 Section 2.2).
/// Prepends the peer's routing identity as the first frame of the returned message.
async fn TransportHub::recv_router(
  self : TransportHub,
) -> Message raise ZmqError {
  let (pipe, raw) = self.recv_fair_queue()
  let out_frames : Array[Bytes] = Array(capacity=raw.frames.length() + 1)
  out_frames.push(pipe.peer_identity)
  for f in raw.frames {
    out_frames.push(f)
  }
  Message::multipart(out_frames)
}


