///|
/// Production ZMTP 3.1 wire-level packet dissector, hex+ASCII trace formatter,
/// sliding-window percentile latency histogram, dual token-bucket traffic shaper,
/// and three-state endpoint circuit breaker (`Closed` / `Open` / `HalfOpen`).

///|
/// High-level classification of a dissected ZMTP 3.1 wire unit.
pub(all) enum DissectedPacketKind {
  /// 64-byte ZMTP 3.1 connection greeting (`mechanism`, `version_major`, `version_minor`, `as_server`).
  GreetingPacket(String, Int32, Int32, Bool)
  /// ZMTP command frame (`command_name`, `summary`).
  CommandPacket(String, String)
  /// ZMTP data frame (`more_flag`, `long_flag`, `payload_len`, `preview`).
  DataFramePacket(Bool, Bool, Int32, String)
  /// Incomplete or malformed trailing wire segment (`byte_len`, `reason`).
  MalformedPacket(Int32, String)
} derive(Eq, Debug)

///|
pub extend DissectedPacketKind with Eq::{not_equal, equal}

///|
pub extend DissectedPacketKind with @debug.Debug::{to_repr}

///|
/// Annotated ZMTP wire record produced by `dissect_zmtp_wire`.
pub struct DissectedPacket {
  offset : Int32
  wire_len : Int32
  kind : DissectedPacketKind
} derive(Eq, Debug)

///|
pub extend DissectedPacket with Eq::{not_equal, equal}

///|
pub extend DissectedPacket with @debug.Debug::{to_repr}

///|
fn preview_printable_ascii(b : Bytes, max_chars : Int32) -> String {
  let limit = if b.length() < max_chars { b.length() } else { max_chars }
  let sb = StringBuilder()
  for i = 0; i < limit; i = i + 1 {
    let code = b[i].to_int() & 0xFF
    if code >= 32 && code <= 126 {
      sb.write_char(Int32::unsafe_to_char(code))
    } else {
      sb.write_char('.')
    }
  }
  if b.length() > max_chars {
    sb.write_string("...")
  }
  sb.to_string()
}

///|
fn summarize_zmtp_command_body(body : Bytes) -> (String, String) {
  let parsed : Result[ZmtpCommand, ZmqError] = Ok(
    ZmtpCommand::decode_body(body),
  ) catch {
    err => Err(err)
  }
  match parsed {
    Ok(Ready(meta)) => {
      let id_desc = match meta.identity {
        Some(id) => bytes_to_hex(id)
        None => "auto"
      }
      ("READY", "Socket-Type=\{meta.socket_type}, Identity=\{id_desc}")
    }
    Ok(Initiate(meta)) => {
      let id_desc = match meta.identity {
        Some(id) => bytes_to_hex(id)
        None => "auto"
      }
      ("INITIATE", "Socket-Type=\{meta.socket_type}, Identity=\{id_desc}")
    }
    Ok(Hello(user, _)) => ("HELLO", "username=\{user}")
    Ok(Welcome) => ("WELCOME", "PLAIN server welcome")
    Ok(ErrorCmd(reason)) => ("ERROR", "reason=\{reason}")
    Ok(Ping(ttl, ctx)) => {
      let ctx_len = ctx.length()
      ("PING", "ttl=\{ttl}, ctx_bytes=\{ctx_len}")
    }
    Ok(Pong(ctx)) => {
      let ctx_len = ctx.length()
      ("PONG", "ctx_bytes=\{ctx_len}")
    }
    Ok(Subscribe(topic)) => {
      let prev = preview_printable_ascii(topic, 24)
      ("SUBSCRIBE", "topic=\{prev}")
    }
    Ok(Cancel(topic)) => {
      let prev = preview_printable_ascii(topic, 24)
      ("CANCEL", "topic=\{prev}")
    }
    Ok(Unknown(name, raw)) => {
      let n = raw.length()
      (name, "unknown_cmd_bytes=\{n}")
    }
    Err(_) => {
      if is_plain_welcome(body) {
        return ("WELCOME", "PLAIN server welcome")
      }
      let plain_hello : Result[(String, String), ZmqError] = Ok(
        decode_plain_hello(body),
      ) catch {
        e => Err(e)
      }
      match plain_hello {
        Ok((u, _)) => ("HELLO", "username=\{u}")
        Err(_) => {
          let plain_init : Result[Metadata, ZmqError] = Ok(
            decode_plain_initiate(body),
          ) catch {
            e => Err(e)
          }
          match plain_init {
            Ok(m) => ("INITIATE", "Socket-Type=\{m.socket_type}")
            Err(_) => ("MALFORMED_CMD", preview_printable_ascii(body, 24))
          }
        }
      }
    }
  }
}

///|
/// Dissect a raw ZMTP 3.1 byte stream (optionally starting with a 64-byte Greeting)
/// into an ordered array of `DissectedPacket` descriptors without raising on malformed tails.
pub fn dissect_zmtp_wire(
  stream : Bytes,
  expect_greeting~ : Bool = false,
) -> Array[DissectedPacket] {
  let out : Array[DissectedPacket] = []
  let total = stream.length()
  let mut pos = 0

  if expect_greeting ||
    (total >= 64 && stream[0] == b'\xFF' && stream[9] == b'\x7F') {
    if total < 64 {
      out.push({
        offset: 0,
        wire_len: total,
        kind: MalformedPacket(total, "truncated 64-byte ZMTP greeting"),
      })
      return out
    }
    let g_slice = slice_bytes(stream, 0, 64)
    let g_res : Result[Greeting, ZmqError] = Ok(Greeting::decode(g_slice)) catch {
      err => Err(err)
    }
    match g_res {
      Ok(g) =>
        out.push({
          offset: 0,
          wire_len: 64,
          kind: GreetingPacket(
            g.mechanism,
            g.version_major,
            g.version_minor,
            g.as_server,
          ),
        })
      Err(_) =>
        out.push({
          offset: 0,
          wire_len: 64,
          kind: MalformedPacket(64, "invalid ZMTP 3.1 greeting signature"),
        })
    }
    pos = 64
  }

  while pos < total {
    let start_offset = pos
    let flags = stream[pos].to_int() & 0xFF
    if (flags & FLAG_RESERVED_MASK) != 0 {
      let rem = total - pos
      out.push({
        offset: start_offset,
        wire_len: rem,
        kind: MalformedPacket(rem, "non-zero reserved flag bits in ZMTP header"),
      })
      break
    }
    let is_more = (flags & FLAG_MORE) != 0
    let is_long = (flags & FLAG_LONG) != 0
    let is_cmd = (flags & FLAG_COMMAND) != 0
    let hdr_len = if is_long { 9 } else { 2 }
    if pos + hdr_len > total {
      let rem = total - pos
      out.push({
        offset: start_offset,
        wire_len: rem,
        kind: MalformedPacket(rem, "truncated ZMTP frame header"),
      })
      break
    }
    let body_len64 = if is_long {
      let mut acc = 0L
      for i = 1; i <= 8; i = i + 1 {
        acc = (acc << 8) | (stream[pos + i].to_int() & 0xFF).to_int64()
      }
      acc
    } else {
      (stream[pos + 1].to_int() & 0xFF).to_int64()
    }
    if body_len64 < 0L || body_len64 > (total - pos - hdr_len).to_int64() {
      let rem = total - pos
      out.push({
        offset: start_offset,
        wire_len: rem,
        kind: MalformedPacket(rem, "truncated ZMTP frame payload"),
      })
      break
    }
    let body_len = body_len64.to_int()
    let body = slice_bytes(stream, pos + hdr_len, pos + hdr_len + body_len)
    let wire_len = hdr_len + body_len
    if is_cmd {
      let (cname, cdesc) = summarize_zmtp_command_body(body)
      out.push({
        offset: start_offset,
        wire_len,
        kind: CommandPacket(cname, cdesc),
      })
    } else {
      let prev = preview_printable_ascii(body, 32)
      out.push({
        offset: start_offset,
        wire_len,
        kind: DataFramePacket(is_more, is_long, body_len, prev),
      })
    }
    pos = pos + wire_len
  }
  out
}

///|
fn pad_hex_u16(val : Int32) -> String {
  let digits = "0123456789abcdef"
  let sb = StringBuilder()
  sb.write_char(
    Int32::unsafe_to_char(digits.code_unit_at((val >> 12) & 0x0F).to_int()),
  )
  sb.write_char(
    Int32::unsafe_to_char(digits.code_unit_at((val >> 8) & 0x0F).to_int()),
  )
  sb.write_char(
    Int32::unsafe_to_char(digits.code_unit_at((val >> 4) & 0x0F).to_int()),
  )
  sb.write_char(Int32::unsafe_to_char(digits.code_unit_at(val & 0x0F).to_int()))
  sb.to_string()
}

///|
/// Format raw `data` into a side-by-side 16-byte hexadecimal + printable ASCII dump
/// (matching `hexdump -C` / Wireshark packet inspection layout).
pub fn format_wire_hexdump(data : Bytes, max_bytes~ : Int32 = 512) -> String {
  let limit = if max_bytes > 0 && data.length() > max_bytes {
    max_bytes
  } else {
    data.length()
  }
  let digits = "0123456789abcdef"
  let sb = StringBuilder()
  let mut row_start = 0
  while row_start < limit {
    sb.write_string(pad_hex_u16(row_start))
    sb.write_string("  ")
    for col = 0; col < 16; col = col + 1 {
      let idx = row_start + col
      if idx < limit {
        let b = data[idx].to_int() & 0xFF
        sb.write_char(
          Int32::unsafe_to_char(digits.code_unit_at((b >> 4) & 0x0F).to_int()),
        )
        sb.write_char(
          Int32::unsafe_to_char(digits.code_unit_at(b & 0x0F).to_int()),
        )
        sb.write_char(' ')
      } else {
        sb.write_string("   ")
      }
      if col == 7 {
        sb.write_char(' ')
      }
    }
    sb.write_string(" |")
    for col = 0; col < 16; col = col + 1 {
      let idx = row_start + col
      if idx < limit {
        let b = data[idx].to_int() & 0xFF
        if b >= 32 && b <= 126 {
          sb.write_char(Int32::unsafe_to_char(b))
        } else {
          sb.write_char('.')
        }
      }
    }
    sb.write_string("|\n")
    row_start = row_start + 16
  }
  sb.to_string()
}

///|
/// Format an array of `DissectedPacket` records into a multi-line human-readable trace.
pub fn format_dissected_trace(packets : Array[DissectedPacket]) -> String {
  let sb = StringBuilder()
  for pkt in packets {
    sb.write_string("[@\{pkt.offset} +\{pkt.wire_len}B] ")
    match pkt.kind {
      GreetingPacket(mech, maj, min, as_srv) => {
        let srv_flag = if as_srv { "server" } else { "client" }
        sb.write_string(
          "GREETING v\{maj}.\{min} mechanism=\{mech} role=\{srv_flag}\n",
        )
      }
      CommandPacket(name, desc) => sb.write_string("CMD \{name} (\{desc})\n")
      DataFramePacket(more, long_f, len, prev) => {
        let m_tag = if more { "MORE" } else { "FINAL" }
        let l_tag = if long_f { "LONG" } else { "SHORT" }
        sb.write_string("DATA [\{m_tag}|\{l_tag}] len=\{len} \"\{prev}\"\n")
      }
      MalformedPacket(len, reason) =>
        sb.write_string("MALFORMED (\{len}B): \{reason}\n")
    }
  }
  sb.to_string()
}

///|
/// Latency distribution snapshot with microsecond percentiles and fixed bucket counts.
pub struct LatencySummary {
  samples : Int64
  min_us : Int64
  max_us : Int64
  mean_us : Int64
  p50_us : Int64
  p90_us : Int64
  p95_us : Int64
  p99_us : Int64
} derive(Eq, Debug)

///|
pub extend LatencySummary with Eq::{not_equal, equal}

///|
pub extend LatencySummary with @debug.Debug::{to_repr}

///|
/// Sliding-window microsecond latency histogram with fixed latency buckets:
/// `[<50us, <100us, <250us, <500us, <1ms, <5ms, <10ms, <50ms, >=50ms]`.
pub struct LatencyHistogram {
  window_capacity : Int32
  priv window : Array[Int64]
  buckets : FixedArray[Int64]
  mut total_samples : Int64
  mut sum_us : Int64
  mut min_us : Int64
  mut max_us : Int64
}

///|
/// Create a new `LatencyHistogram` retaining the latest `window_capacity` samples
/// for exact percentile calculation alongside lifetime cumulative buckets.
pub fn LatencyHistogram::new(
  window_capacity~ : Int32 = 2048,
) -> LatencyHistogram {
  let cap = if window_capacity <= 0 { 128 } else { window_capacity }
  {
    window_capacity: cap,
    window: Array(capacity=cap),
    buckets: FixedArray::make(9, 0L),
    total_samples: 0L,
    sum_us: 0L,
    min_us: 0L,
    max_us: 0L,
  }
}

///|
fn bucket_index_for_us(us : Int64) -> Int32 {
  if us < 50L {
    0
  } else if us < 100L {
    1
  } else if us < 250L {
    2
  } else if us < 500L {
    3
  } else if us < 1000L {
    4
  } else if us < 5000L {
    5
  } else if us < 10000L {
    6
  } else if us < 50000L {
    7
  } else {
    8
  }
}

///|
/// Record a single latency observation in microseconds (`>= 0`).
pub fn LatencyHistogram::record_us(
  self : LatencyHistogram,
  latency_us : Int64,
) -> Unit {
  let val = if latency_us < 0L { 0L } else { latency_us }
  if self.total_samples == 0L {
    self.min_us = val
    self.max_us = val
  } else {
    if val < self.min_us {
      self.min_us = val
    }
    if val > self.max_us {
      self.max_us = val
    }
  }
  self.total_samples = self.total_samples + 1L
  self.sum_us = self.sum_us + val
  let b_idx = bucket_index_for_us(val)
  self.buckets[b_idx] = self.buckets[b_idx] + 1L

  if self.window.length() >= self.window_capacity {
    let _ = self.window.remove(0)
  }
  self.window.push(val)
}

///|
/// Record a latency observation given in milliseconds.
pub fn LatencyHistogram::record_ms(
  self : LatencyHistogram,
  latency_ms : Int64,
) -> Unit {
  self.record_us(latency_ms * 1000L)
}

///|
fn sort_i64_copy(src : Array[Int64]) -> Array[Int64] {
  let arr : Array[Int64] = Array(capacity=src.length())
  for v in src {
    arr.push(v)
  }
  // Insertion sort (fast and allocation-free on sliding windows <= a few thousand samples)
  for i = 1; i < arr.length(); i = i + 1 {
    let key = arr[i]
    let mut j = i - 1
    while j >= 0 && arr[j] > key {
      arr[j + 1] = arr[j]
      j = j - 1
    }
    arr[j + 1] = key
  }
  arr
}

///|
fn percentile_from_sorted(sorted : Array[Int64], pct_num : Int32) -> Int64 {
  let n = sorted.length()
  if n == 0 {
    return 0L
  }
  let rank = (n * pct_num + 99) / 100 - 1
  let clamped = if rank < 0 { 0 } else if rank >= n { n - 1 } else { rank }
  sorted[clamped]
}

///|
/// Compute a `LatencySummary` containing lifetime min/max/mean and sliding-window
/// p50, p90, p95, and p99 percentiles in microseconds.
pub fn LatencyHistogram::summarize(self : LatencyHistogram) -> LatencySummary {
  if self.total_samples == 0L {
    return {
      samples: 0L,
      min_us: 0L,
      max_us: 0L,
      mean_us: 0L,
      p50_us: 0L,
      p90_us: 0L,
      p95_us: 0L,
      p99_us: 0L,
    }
  }
  let sorted = sort_i64_copy(self.window)
  let mean = self.sum_us / self.total_samples
  {
    samples: self.total_samples,
    min_us: self.min_us,
    max_us: self.max_us,
    mean_us: mean,
    p50_us: percentile_from_sorted(sorted, 50),
    p90_us: percentile_from_sorted(sorted, 90),
    p95_us: percentile_from_sorted(sorted, 95),
    p99_us: percentile_from_sorted(sorted, 99),
  }
}

///|
/// Reset all recorded observations and bucket counters in this `LatencyHistogram`.
pub fn LatencyHistogram::reset(self : LatencyHistogram) -> Unit {
  self.window.clear()
  for i = 0; i < self.buckets.length(); i = i + 1 {
    self.buckets[i] = 0L
  }
  self.total_samples = 0L
  self.sum_us = 0L
  self.min_us = 0L
  self.max_us = 0L
}

///|
/// Dual token-bucket rate limiter shaping both message rate (`msgs_per_sec`) and
/// byte bandwidth (`bytes_per_sec`, `0L` = unlimited byte rate).
///
/// Uses millisecond token refill with `1000x` internal scaling to avoid floating-point drift.
pub struct RateLimiter {
  msgs_per_sec : Int64
  burst_msgs : Int64
  bytes_per_sec : Int64
  burst_bytes : Int64
  priv mut msg_millitokens : Int64
  priv mut byte_millitokens : Int64
  priv mut last_refill_ms : Int64
  mut accepted_msgs : Int64
  mut rejected_msgs : Int64
}

///|
/// Create a new `RateLimiter`.
/// - `msgs_per_sec`: sustained message rate (`<= 0L` disables message rate limiting).
/// - `burst_msgs`: maximum message burst bucket size.
/// - `bytes_per_sec`: sustained payload byte rate (`<= 0L` disables bandwidth limiting).
/// - `burst_bytes`: maximum byte burst bucket size.
pub fn RateLimiter::new(
  msgs_per_sec~ : Int64 = 0L,
  burst_msgs~ : Int64 = 0L,
  bytes_per_sec~ : Int64 = 0L,
  burst_bytes~ : Int64 = 0L,
  initial_now_ms~ : Int64 = 0L,
) -> RateLimiter {
  let eff_b_msgs = if msgs_per_sec > 0L {
    if burst_msgs >= 1L {
      burst_msgs
    } else {
      msgs_per_sec
    }
  } else {
    0L
  }
  let eff_b_bytes = if bytes_per_sec > 0L {
    if burst_bytes >= 1L {
      burst_bytes
    } else {
      bytes_per_sec
    }
  } else {
    0L
  }
  {
    msgs_per_sec,
    burst_msgs: eff_b_msgs,
    bytes_per_sec,
    burst_bytes: eff_b_bytes,
    msg_millitokens: eff_b_msgs * 1000L,
    byte_millitokens: eff_b_bytes * 1000L,
    last_refill_ms: initial_now_ms,
    accepted_msgs: 0L,
    rejected_msgs: 0L,
  }
}

///|
fn RateLimiter::refill_at(self : RateLimiter, now_ms : Int64) -> Unit {
  if now_ms <= self.last_refill_ms {
    return
  }
  let elapsed_ms = now_ms - self.last_refill_ms
  self.last_refill_ms = now_ms
  if self.msgs_per_sec > 0L {
    let cap = self.burst_msgs * 1000L
    let added = elapsed_ms * self.msgs_per_sec
    let next_val = self.msg_millitokens + added
    self.msg_millitokens = if next_val > cap { cap } else { next_val }
  }
  if self.bytes_per_sec > 0L {
    let cap = self.burst_bytes * 1000L
    let added = elapsed_ms * self.bytes_per_sec
    let next_val = self.byte_millitokens + added
    self.byte_millitokens = if next_val > cap { cap } else { next_val }
  }
}

///|
/// Attempt to consume quota for 1 message of `payload_bytes` at `now_ms`.
/// Returns `true` if permitted, or `false` (leaving tokens intact) if rate-limited.
pub fn RateLimiter::try_acquire(
  self : RateLimiter,
  payload_bytes : Int32,
  now_ms : Int64,
) -> Bool {
  self.refill_at(now_ms)
  let need_msg = if self.msgs_per_sec > 0L { 1000L } else { 0L }
  let clean_bytes = (if payload_bytes < 0 { 0 } else { payload_bytes }).to_int64()
  let need_bytes = if self.bytes_per_sec > 0L {
    clean_bytes * 1000L
  } else {
    0L
  }
  if self.msgs_per_sec > 0L && self.msg_millitokens < need_msg {
    self.rejected_msgs = self.rejected_msgs + 1L
    return false
  }
  if self.bytes_per_sec > 0L && self.byte_millitokens < need_bytes {
    self.rejected_msgs = self.rejected_msgs + 1L
    return false
  }
  self.msg_millitokens = self.msg_millitokens - need_msg
  self.byte_millitokens = self.byte_millitokens - need_bytes
  self.accepted_msgs = self.accepted_msgs + 1L
  true
}

///|
/// Estimate how many milliseconds a caller must wait from `now_ms` until a message
/// of `payload_bytes` will be permitted. Returns `0` if immediately allowed.
pub fn RateLimiter::wait_time_ms(
  self : RateLimiter,
  payload_bytes : Int32,
  now_ms : Int64,
) -> Int32 {
  self.refill_at(now_ms)
  let mut wait_msg_ms = 0L
  if self.msgs_per_sec > 0L && self.msg_millitokens < 1000L {
    let deficit = 1000L - self.msg_millitokens
    wait_msg_ms = (deficit + self.msgs_per_sec - 1L) / self.msgs_per_sec
  }
  let mut wait_byte_ms = 0L
  let clean_bytes = (if payload_bytes < 0 { 0 } else { payload_bytes }).to_int64()
  if self.bytes_per_sec > 0L {
    let need = clean_bytes * 1000L
    if self.byte_millitokens < need {
      let deficit = need - self.byte_millitokens
      wait_byte_ms = (deficit + self.bytes_per_sec - 1L) / self.bytes_per_sec
    }
  }
  let max_wait = if wait_msg_ms > wait_byte_ms {
    wait_msg_ms
  } else {
    wait_byte_ms
  }
  if max_wait > 60000L {
    60000
  } else {
    max_wait.to_int()
  }
}

///|
/// Asynchronously wait until tokens become available and send `msg` on `socket`.
pub async fn RateLimiter::send_paced(
  self : RateLimiter,
  socket : Socket,
  msg : Message,
  policy~ : HwmPolicy = BlockOnFull,
) -> Unit raise ZmqError {
  let sz = msg.total_bytes()
  while !socket.is_closed() {
    let now = @async.now()
    if self.try_acquire(sz, now) {
      socket.send(msg, policy~)
      return
    }
    let delay = self.wait_time_ms(sz, now)
    let step = if delay <= 0 { 1 } else { delay }
    @async.sleep(step)
  }
  raise Closed
}

///|
/// Operational state of an endpoint `CircuitBreaker`.
pub(all) enum CircuitState {
  /// Normal operation; requests pass through and consecutive failures are counted.
  CircuitClosed
  /// Tripped after `failure_threshold` failures; fast-fails requests until `cooldown_ms` elapses.
  CircuitOpen
  /// Probe state allowing limited trial requests until `success_threshold` consecutive probes succeed.
  CircuitHalfOpen
} derive(Eq, Debug)

///|
pub extend CircuitState with Eq::{not_equal, equal}

///|
pub extend CircuitState with @debug.Debug::{to_repr}

///|
/// Three-state endpoint `CircuitBreaker` (`CircuitClosed` / `CircuitOpen` / `CircuitHalfOpen`)
/// protecting distributed ZeroMQ RPC pipelines against cascading peer stalls.
pub struct CircuitBreaker {
  failure_threshold : Int32
  success_threshold : Int32
  cooldown_ms : Int64
  priv mut state : CircuitState
  priv mut consecutive_failures : Int32
  priv mut half_open_successes : Int32
  priv mut opened_at_ms : Int64
  mut trip_count : Int64
  mut rejected_calls : Int64
}

///|
/// Create a new `CircuitBreaker` starting in `CircuitClosed`.
pub fn CircuitBreaker::new(
  failure_threshold~ : Int32 = 5,
  success_threshold~ : Int32 = 2,
  cooldown_ms~ : Int64 = 5000L,
) -> CircuitBreaker {
  let f_th = if failure_threshold <= 0 { 1 } else { failure_threshold }
  let s_th = if success_threshold <= 0 { 1 } else { success_threshold }
  let cd = if cooldown_ms <= 0L { 100L } else { cooldown_ms }
  {
    failure_threshold: f_th,
    success_threshold: s_th,
    cooldown_ms: cd,
    state: CircuitClosed,
    consecutive_failures: 0,
    half_open_successes: 0,
    opened_at_ms: 0L,
    trip_count: 0L,
    rejected_calls: 0L,
  }
}

///|
/// Inspect the current `CircuitState` (advancing `CircuitOpen -> CircuitHalfOpen` if
/// `cooldown_ms` has elapsed as of `now_ms`).
pub fn CircuitBreaker::current_state(
  self : CircuitBreaker,
  now_ms~ : Int64 = -1L,
) -> CircuitState {
  if self.state == CircuitOpen && now_ms >= 0L {
    if now_ms - self.opened_at_ms >= self.cooldown_ms {
      self.state = CircuitHalfOpen
      self.half_open_successes = 0
    }
  }
  self.state
}

///|
/// Check whether an outbound call is allowed at `now_ms`.
/// Automatically transitions `CircuitOpen` to `CircuitHalfOpen` once `cooldown_ms` expires.
pub fn CircuitBreaker::allow_request(
  self : CircuitBreaker,
  now_ms : Int64,
) -> Bool {
  match self.current_state(now_ms~) {
    CircuitClosed | CircuitHalfOpen => true
    CircuitOpen => {
      self.rejected_calls = self.rejected_calls + 1L
      false
    }
  }
}

///|
/// Record a successful downstream operation, resetting failure streaks or advancing
/// `CircuitHalfOpen` toward `CircuitClosed`.
pub fn CircuitBreaker::record_success(self : CircuitBreaker) -> Unit {
  match self.state {
    CircuitClosed => self.consecutive_failures = 0
    CircuitHalfOpen => {
      self.half_open_successes = self.half_open_successes + 1
      if self.half_open_successes >= self.success_threshold {
        self.state = CircuitClosed
        self.consecutive_failures = 0
        self.half_open_successes = 0
      }
    }
    CircuitOpen => ()
  }
}

///|
/// Record a downstream timeout or error at `now_ms`, tripping the breaker to
/// `CircuitOpen` when `failure_threshold` is reached (or immediately if `CircuitHalfOpen`).
pub fn CircuitBreaker::record_failure(
  self : CircuitBreaker,
  now_ms : Int64,
) -> Unit {
  match self.state {
    CircuitClosed => {
      self.consecutive_failures = self.consecutive_failures + 1
      if self.consecutive_failures >= self.failure_threshold {
        self.state = CircuitOpen
        self.opened_at_ms = now_ms
        self.trip_count = self.trip_count + 1L
      }
    }
    CircuitHalfOpen => {
      self.state = CircuitOpen
      self.opened_at_ms = now_ms
      self.half_open_successes = 0
      self.trip_count = self.trip_count + 1L
    }
    CircuitOpen => self.opened_at_ms = now_ms
  }
}

///|
/// Forcefully reset this `CircuitBreaker` back to `CircuitClosed`.
pub fn CircuitBreaker::reset(self : CircuitBreaker) -> Unit {
  self.state = CircuitClosed
  self.consecutive_failures = 0
  self.half_open_successes = 0
}
