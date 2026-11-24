///|
/// ZeroMQ RFC 4 ZPL (`ZeroMQ Property Language`) hierarchical configuration
/// parser/serializer (`ZConfig`), CZMQ `ZActor` actor concurrency model, and
/// timer + socket event-driven `ZLoop` reactor.

///|
/// Hierarchical configuration node implementing ZeroMQ RFC 4 (ZPL).
pub struct ZConfig {
  name : String
  mut value : String
  children : Array[ZConfig]
} derive(Eq, Debug)

///|
pub extend ZConfig with Eq::{not_equal, equal}

///|
pub extend ZConfig with @debug.Debug::{to_repr}

///|
/// Create a new `ZConfig` node with `name` and optional string `value`.
pub fn ZConfig::new(name : String, value~ : String = "") -> ZConfig {
  { name, value, children: [], }
}

///|
/// Create an unnamed root `ZConfig` container node.
pub fn ZConfig::root() -> ZConfig {
  ZConfig::new("root")
}

///|
fn is_horizontal_ws(ch : Int32) -> Bool {
  ch == 32 || ch == 9
}

///|
fn trim_ascii_ws(s : String) -> String {
  let mut start = 0
  let mut end = s.length()
  while start < end && is_horizontal_ws(s.code_unit_at(start).to_int()) {
    start = start + 1
  }
  while end > start &&
        (
          is_horizontal_ws(s.code_unit_at(end - 1).to_int()) ||
          s.code_unit_at(end - 1).to_int() == 13
        ) {
    end = end - 1
  }
  if start >= end {
    ""
  } else {
    s[start:end].to_owned()
  }
}

///|
fn strip_zpl_comment(raw : String) -> String {
  let mut in_single = false
  let mut in_double = false
  for i = 0; i < raw.length(); i = i + 1 {
    let ch = raw.code_unit_at(i).to_int()
    if ch == 39 && !in_double {
      in_single = !in_single
    } else if ch == 34 && !in_single {
      in_double = !in_double
    } else if ch == 35 && !in_single && !in_double {
      return raw[:i].to_owned()
    }
  }
  raw
}

///|
fn unquote_zpl_value(raw_val : String) -> String raise ZmqError {
  let trimmed = trim_ascii_ws(raw_val)
  if trimmed.is_empty() {
    return ""
  }
  let first = trimmed.code_unit_at(0).to_int()
  if first == 34 || first == 39 {
    if trimmed.length() < 2 ||
      trimmed.code_unit_at(trimmed.length() - 1).to_int() != first {
      raise ProtocolError("unterminated quoted value in ZPL: \{trimmed}")
    }
    return trimmed[1:trimmed.length() - 1].to_owned()
  }
  trimmed
}

///|
/// Parse a ZeroMQ RFC 4 ZPL configuration document into a `ZConfig` tree.
///
/// Indentation uses 4 spaces per hierarchy level (`level = leading_spaces / 4`).
/// Comments start with `#` outside quotes, and properties take the form `name` or `name = value`.
pub fn ZConfig::parse(text : String) -> ZConfig raise ZmqError {
  let root = ZConfig::root()
  let stack : Array[(Int32, ZConfig)] = [(-1, root)]
  let mut line_start = 0
  let mut line_no = 0
  let total_len = text.length()
  while line_start <= total_len {
    let mut line_end = line_start
    while line_end < total_len && text.code_unit_at(line_end).to_int() != 10 {
      line_end = line_end + 1
    }
    line_no = line_no + 1
    let raw_line = text[line_start:line_end].to_owned()
    line_start = line_end + 1

    let uncommented = strip_zpl_comment(raw_line)
    let trimmed = trim_ascii_ws(uncommented)
    if trimmed.is_empty() {
      continue
    }
    // Count leading spaces (tabs rejected per RFC 4 strict 4-space rule)
    let mut spaces = 0
    while spaces < uncommented.length() {
      let c = uncommented.code_unit_at(spaces).to_int()
      if c == 32 {
        spaces = spaces + 1
      } else if c == 9 {
        raise ProtocolError(
          "ZPL line \{line_no}: tabs are not permitted for indentation (use 4 spaces)",
        )
      } else {
        break
      }
    }
    if spaces % 4 != 0 {
      raise ProtocolError(
        "ZPL line \{line_no}: indentation \{spaces} is not a multiple of 4 spaces",
      )
    }
    let level = spaces / 4

    // Split `name = value` or bare section `name`
    let mut eq_idx = -1
    for i = 0; i < trimmed.length(); i = i + 1 {
      if trimmed.code_unit_at(i).to_int() == 61 {
        eq_idx = i
        break
      }
    }
    let (prop_name, prop_val) = if eq_idx >= 0 {
      let lhs = trim_ascii_ws(trimmed[:eq_idx].to_owned())
      let rhs = unquote_zpl_value(trimmed[eq_idx + 1:].to_owned())
      (lhs, rhs)
    } else {
      (trimmed, "")
    }
    if prop_name.is_empty() {
      raise ProtocolError("ZPL line \{line_no}: empty property name")
    }

    while stack.length() > 1 {
      let (top_lvl, _) = stack[stack.length() - 1]
      if top_lvl >= level {
        let _ = stack.pop()
      } else {
        break
      }
    }
    let (parent_lvl, parent_node) = stack[stack.length() - 1]
    if level > parent_lvl + 1 {
      raise ProtocolError(
        "ZPL line \{line_no}: indentation jumped more than one level",
      )
    }
    let node = ZConfig::new(prop_name, value=prop_val)
    parent_node.children.push(node)
    stack.push((level, node))
  }
  root
}

///|
fn split_config_path(path : String) -> Array[String] {
  let parts : Array[String] = []
  let mut start = 0
  for i = 0; i <= path.length(); i = i + 1 {
    if i == path.length() || path.code_unit_at(i).to_int() == 47 {
      if i > start {
        parts.push(path[start:i].to_owned())
      }
      start = i + 1
    }
  }
  parts
}

///|
/// Locate a descendant `ZConfig` node by slash-separated `path` (e.g. `"context/iothreads"`).
pub fn ZConfig::locate(self : ZConfig, path : String) -> ZConfig? {
  let parts = split_config_path(path)
  let mut cur = self
  for seg in parts {
    let mut next_opt : ZConfig? = None
    for child in cur.children {
      if child.name == seg {
        next_opt = Some(child)
        break
      }
    }
    match next_opt {
      Some(n) => cur = n
      None => return None
    }
  }
  Some(cur)
}

///|
/// Resolve a string value at `path`, returning `default` if the path does not exist.
pub fn ZConfig::get(
  self : ZConfig,
  path : String,
  default~ : String = "",
) -> String {
  match self.locate(path) {
    Some(node) => node.value
    None => default
  }
}

///|
/// Resolve an `Int32` value at `path`, returning `default` if missing or invalid.
pub fn ZConfig::get_int(
  self : ZConfig,
  path : String,
  default~ : Int32 = 0,
) -> Int32 {
  match self.locate(path) {
    Some(node) => @string.parse_int(node.value) catch { _ => default }
    None => default
  }
}

///|
/// Resolve a boolean value (`"1"`, `"true"`, `"yes"`, `"on"`) at `path`.
pub fn ZConfig::get_bool(
  self : ZConfig,
  path : String,
  default~ : Bool = false,
) -> Bool {
  match self.locate(path) {
    Some(node) => {
      let up = ascii_to_upper(trim_ascii_ws(node.value))
      if up == "1" || up == "TRUE" || up == "YES" || up == "ON" {
        true
      } else if up == "0" || up == "FALSE" || up == "NO" || up == "OFF" {
        false
      } else {
        default
      }
    }
    None => default
  }
}

///|
/// Insert or update the value at slash-separated `path`, creating intermediate
/// section nodes as needed.
pub fn ZConfig::put(self : ZConfig, path : String, value : String) -> Unit {
  let parts = split_config_path(path)
  if parts.is_empty() {
    self.value = value
    return
  }
  let mut cur = self
  for seg in parts {
    let mut found : ZConfig? = None
    for child in cur.children {
      if child.name == seg {
        found = Some(child)
        break
      }
    }
    match found {
      Some(existing) => cur = existing
      None => {
        let created = ZConfig::new(seg)
        cur.children.push(created)
        cur = created
      }
    }
  }
  cur.value = value
}

///|
fn needs_zpl_quotes(val : String) -> Bool {
  if val.is_empty() {
    return false
  }
  if is_horizontal_ws(val.code_unit_at(0).to_int()) ||
    is_horizontal_ws(val.code_unit_at(val.length() - 1).to_int()) {
    return true
  }
  for i = 0; i < val.length(); i = i + 1 {
    let c = val.code_unit_at(i).to_int()
    if c == 35 || c == 61 {
      return true
    }
  }
  false
}

///|
fn write_zpl_node(node : ZConfig, level : Int32, sb : StringBuilder) -> Unit {
  for i = 0; i < level * 4; i = i + 1 {
    sb.write_char(' ')
  }
  sb.write_string(node.name)
  if !node.value.is_empty() {
    sb.write_string(" = ")
    if needs_zpl_quotes(node.value) {
      sb.write_char('"')
      sb.write_string(node.value)
      sb.write_char('"')
    } else {
      sb.write_string(node.value)
    }
  }
  sb.write_char('\n')
  for child in node.children {
    write_zpl_node(child, level + 1, sb)
  }
}

///|
/// Serialize this `ZConfig` hierarchy into canonical RFC 4 ZPL text.
pub fn ZConfig::to_zpl(self : ZConfig) -> String {
  let sb = StringBuilder()
  if self.name == "root" && self.value.is_empty() {
    for child in self.children {
      write_zpl_node(child, 0, sb)
    }
  } else {
    write_zpl_node(self, 0, sb)
  }
  sb.to_string()
}

///|
/// Apply a `ZConfig` subtree (`socket/...` or root properties) onto a `SocketOptions` instance.
pub fn apply_zconfig_to_socket_options(
  cfg : ZConfig,
  opts : SocketOptions,
) -> Unit {
  let base = match cfg.locate("socket") {
    Some(sub) => sub
    None => cfg
  }
  let snd = base.get_int("sndhwm", default=opts.sndhwm)
  let rcv = base.get_int("rcvhwm", default=opts.rcvhwm)
  opts.set_sndhwm(snd)
  opts.set_rcvhwm(rcv)

  let ident = base.get("identity")
  if !ident.is_empty() {
    opts.set_identity(Some(string_to_ascii_bytes(ident)))
  }
  let reconn_ivl = base.get_int(
    "reconnect_ivl_ms",
    default=opts.reconnect_ivl_ms,
  )
  let reconn_max = base.get_int(
    "reconnect_ivl_max_ms",
    default=opts.reconnect_ivl_max_ms,
  )
  opts.set_reconnect_interval(reconn_ivl, max_ms=reconn_max)

  let hb_ivl = base.get_int("heartbeat_ivl_ms", default=opts.heartbeat_ivl_ms)
  let hb_to = base.get_int(
    "heartbeat_timeout_ms",
    default=opts.heartbeat_timeout_ms,
  )
  let hb_ttl = base.get_int("heartbeat_ttl_ms", default=opts.heartbeat_ttl_ms)
  opts.set_heartbeat(ivl_ms=hb_ivl, timeout_ms=hb_to, ttl_ms=hb_ttl)

  opts.set_router_mandatory(
    base.get_bool("router_mandatory", default=opts.router_mandatory),
  )
  opts.set_xpub_verbose(
    base.get_bool("xpub_verbose", default=opts.xpub_verbose),
  )
  opts.max_frame_size = base.get_int(
    "max_frame_size",
    default=opts.max_frame_size,
  )

  if base.get_bool("plain/server", default=false) {
    opts.set_plain_server(true)
  } else {
    let p_user = base.get("plain/username")
    if !p_user.is_empty() {
      let p_pass = base.get("plain/password")
      opts.set_plain_credentials(p_user, p_pass)
    }
  }
}

///|
/// Export a `SocketOptions` configuration into a `ZConfig` RFC 4 tree under `"socket"`.
pub fn socket_options_to_zconfig(opts : SocketOptions) -> ZConfig {
  let root = ZConfig::root()
  root.put("socket/sndhwm", "\{opts.sndhwm}")
  root.put("socket/rcvhwm", "\{opts.rcvhwm}")
  root.put("socket/reconnect_ivl_ms", "\{opts.reconnect_ivl_ms}")
  root.put("socket/reconnect_ivl_max_ms", "\{opts.reconnect_ivl_max_ms}")
  root.put("socket/heartbeat_ivl_ms", "\{opts.heartbeat_ivl_ms}")
  root.put("socket/heartbeat_timeout_ms", "\{opts.heartbeat_timeout_ms}")
  root.put("socket/heartbeat_ttl_ms", "\{opts.heartbeat_ttl_ms}")
  let rm_flag = if opts.router_mandatory { "1" } else { "0" }
  let xv_flag = if opts.xpub_verbose { "1" } else { "0" }
  root.put("socket/router_mandatory", rm_flag)
  root.put("socket/xpub_verbose", xv_flag)
  root.put("socket/max_frame_size", "\{opts.max_frame_size}")
  match opts.identity {
    Some(id) => root.put("socket/identity", ascii_bytes_to_string(id))
    None => ()
  }
  root
}

///|
let next_actor_seq : Ref[Int32] = { val: 1, }

///|
/// Canonical termination command string used by CZMQ `zactor_t`.
pub const ZACTOR_TERM_CMD : String = "$TERM"

///|
/// CZMQ-style `ZActor` wrapping a background coroutine connected to its owner
/// through a dedicated `inproc://` `PAIR` control pipe.
pub struct ZActor {
  id : Int32
  pipe : Socket
  priv task : @async.Task[Unit]
  mut stopped : Bool
}

///|
/// Spawn a new `ZActor` inside `group` and `ctx`.
///
/// Automatically binds an exclusive `inproc://zactor-<id>` `PAIR` endpoint, connects
/// the actor body's `pipe`, waits for the initial readiness signal from the actor
/// coroutine, and returns the parent `ZActor` handle.
pub async fn ZActor::spawn(
  group : @async.TaskGroup,
  ctx : Context,
  actor_fn : async (Socket) -> Unit,
) -> ZActor raise ZmqError {
  let id = next_actor_seq.val
  next_actor_seq.val = next_actor_seq.val + 1
  let endpoint = "inproc://zactor-pipe-\{id}"
  let parent_pipe = ctx.socket(Pair)
  let child_pipe = ctx.socket(Pair)
  let _ = parent_pipe.bind(group, endpoint)
  child_pipe.connect(group, endpoint)

  let task = group.spawn(no_wait=true, allow_failure=true, () => {
    defer child_pipe.close()
    // Signal startup readiness to parent `ZActor::spawn` per CZMQ zactor protocol
    child_pipe.send_string("$READY") catch {
      _ => return
    }
    actor_fn(child_pipe) catch {
      _ => ()
    }
  })

  let ready_msg = parent_pipe.recv_string()
  if ready_msg != "$READY" {
    parent_pipe.close()
    task.cancel()
    raise ProtocolError("ZActor failed startup readiness handshake")
  }
  { id, pipe: parent_pipe, task, stopped: false, }
}

///|
/// Return the parent `PAIR` control `Socket` of this `ZActor` (compatible with `Poller` & `ZLoop`).
pub fn ZActor::socket(self : ZActor) -> Socket {
  self.pipe
}

///|
/// Send a control or data `Message` to the running actor.
pub async fn ZActor::send(self : ZActor, msg : Message) -> Unit raise ZmqError {
  if self.stopped {
    raise Closed
  }
  self.pipe.send(msg)
}

///|
/// Send a single-frame command string to the running actor.
pub async fn ZActor::send_string(
  self : ZActor,
  cmd : String,
) -> Unit raise ZmqError {
  self.send(Message::from_string(cmd))
}

///|
/// Receive the next `Message` emitted by the running actor.
pub async fn ZActor::recv(self : ZActor) -> Message raise ZmqError {
  self.pipe.recv()
}

///|
/// Receive the next single-frame `String` emitted by the running actor.
pub async fn ZActor::recv_string(self : ZActor) -> String raise ZmqError {
  self.pipe.recv_string()
}

///|
/// Gracefully request actor termination by sending `"$TERM"`, waiting briefly for
/// the actor coroutine to exit, and closing the parent pipe.
pub async fn ZActor::stop(self : ZActor) -> Unit {
  if self.stopped {
    return
  }
  self.stopped = true
  self.pipe.send_string(ZACTOR_TERM_CMD) catch {
    _ => ()
  }
  @async.sleep(10)
  self.pipe.close()
  self.task.cancel()
}

///|
/// Registered repeating or bounded timer entry inside `ZLoop`.
priv struct ZLoopTimer {
  id : Int32
  interval_ms : Int64
  mut remaining_runs : Int32 // `0` means infinite repeats
  mut next_fire_ms : Int64
  handler : (Int32) -> Bool
}

///|
/// Registered readable socket handler inside `ZLoop`.
priv struct ZLoopReader {
  socket : Socket
  handler : async (Socket) -> Bool
}

///|
/// CZMQ-style `ZLoop` reactor multiplexing readable `Socket` handlers with
/// millisecond-precision one-shot and repeating timers.
///
/// Returning `false` from any timer or reader callback cleanly halts the reactor loop.
pub struct ZLoop {
  priv timers : Array[ZLoopTimer]
  priv readers : Array[ZLoopReader]
  priv mut next_timer_id : Int32
  mut tick_count : Int64
  mut halted : Bool
}

///|
/// Create a new `ZLoop` event reactor.
pub fn ZLoop::new() -> ZLoop {
  { timers: [], readers: [], next_timer_id: 1, tick_count: 0L, halted: false, }
}

///|
/// Register a timer firing every `delay_ms` milliseconds.
/// - `times = 0`: repeats indefinitely until cancelled or reactor stops.
/// - `times > 0`: fires at most `times` times and auto-removes itself.
/// If `handler(timer_id)` returns `false`, the `ZLoop` halts cleanly.
pub fn ZLoop::add_timer(
  self : ZLoop,
  delay_ms : Int32,
  times~ : Int32 = 0,
  now_ms~ : Int64 = -1L,
  handler : (Int32) -> Bool,
) -> Int32 {
  let id = self.next_timer_id
  self.next_timer_id = self.next_timer_id + 1
  let eff_delay = (if delay_ms <= 0 { 1 } else { delay_ms }).to_int64()
  let base_now = if now_ms >= 0L { now_ms } else { @async.now() }
  let runs = if times <= 0 { 0 } else { times }
  self.timers.push({
    id,
    interval_ms: eff_delay,
    remaining_runs: runs,
    next_fire_ms: base_now + eff_delay,
    handler,
  })
  id
}

///|
/// Cancel a registered timer by `timer_id`. Returns `true` if removed.
pub fn ZLoop::cancel_timer(self : ZLoop, timer_id : Int32) -> Bool {
  for i = 0; i < self.timers.length(); i = i + 1 {
    if self.timers[i].id == timer_id {
      let _ = self.timers.remove(i)
      return true
    }
  }
  false
}

///|
/// Register an asynchronous `POLLIN` reader callback for `socket`.
/// If `handler(socket)` returns `false`, the `ZLoop` halts cleanly.
pub fn ZLoop::add_reader(
  self : ZLoop,
  socket : Socket,
  handler : async (Socket) -> Bool,
) -> Unit {
  let _ = self.remove_reader(socket)
  self.readers.push({ socket, handler, })
}

///|
/// Remove a previously registered `socket` reader from this `ZLoop`.
pub fn ZLoop::remove_reader(self : ZLoop, socket : Socket) -> Bool {
  for i = 0; i < self.readers.length(); i = i + 1 {
    if self.readers[i].socket.id == socket.id {
      let _ = self.readers.remove(i)
      return true
    }
  }
  false
}

///|
/// Signal this `ZLoop` to stop at the end of the current reactor iteration.
pub fn ZLoop::stop(self : ZLoop) -> Unit {
  self.halted = true
}

///|
/// Run the `ZLoop` reactor until halted by `stop()`, a callback returning `false`,
/// or until both `timers` and `readers` become empty.
pub async fn ZLoop::run(self : ZLoop) -> Unit {
  self.halted = false
  let poller = Poller::new()
  while !self.halted && (!self.timers.is_empty() || !self.readers.is_empty()) {
    self.tick_count = self.tick_count + 1L
    let now = @async.now()

    // Compute shortest wait until next timer deadline
    let mut wait_ms : Int32 = if self.timers.is_empty() { 50 } else { 50 }
    for t in self.timers {
      let diff = t.next_fire_ms - now
      if diff <= 0L {
        wait_ms = 0
        break
      } else if diff < wait_ms.to_int64() {
        wait_ms = diff.to_int()
      }
    }

    poller.clear()
    for r in self.readers {
      poller.add(r.socket, POLLIN)
    }

    if !self.readers.is_empty() {
      let _ = poller.poll(timeout_ms=wait_ms)
    } else if wait_ms > 0 {
      @async.sleep(wait_ms)
    }

    // Dispatch ready socket readers
    let reader_snapshot : Array[ZLoopReader] = Array(
      capacity=self.readers.length(),
    )
    for r in self.readers {
      reader_snapshot.push(r)
    }
    for r in reader_snapshot {
      if self.halted {
        break
      }
      if poller.has_in(r.socket) {
        let cont = (r.handler)(r.socket)
        if !cont {
          self.halted = true
          break
        }
      }
    }
    if self.halted {
      break
    }

    // Dispatch due timers
    let after_io_now = @async.now()
    let mut idx = 0
    while idx < self.timers.length() && !self.halted {
      let t = self.timers[idx]
      if after_io_now >= t.next_fire_ms {
        let keep_loop = (t.handler)(t.id)
        if !keep_loop {
          self.halted = true
          break
        }
        // Check if handler cancelled itself during invocation
        if idx < self.timers.length() && self.timers[idx].id == t.id {
          if t.remaining_runs > 0 {
            t.remaining_runs = t.remaining_runs - 1
            if t.remaining_runs == 0 {
              let _ = self.timers.remove(idx)
              continue
            }
          }
          t.next_fire_ms = after_io_now + t.interval_ms
          idx = idx + 1
        }
      } else {
        idx = idx + 1
      }
    }
  }
}
