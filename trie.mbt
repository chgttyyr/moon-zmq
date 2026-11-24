///|
/// Prefix Subscription Multi-Trie (`SubscriptionTrie`), Last-Value Cache (`LastValueCache`),
/// LVC Broker Proxy, and `ZMQ_CONFLATE` latest-message buffer (RFC 29 & RFC 47).

///|
/// Per-pipe subscription reference entry inside a `TrieNode`.
priv struct PipeSubCount {
  pipe_id : Int32
  mut count : Int32
}

///|
/// Internal node of the byte-wise prefix `SubscriptionTrie`.
priv struct TrieNode {
  edges : Array[(Byte, TrieNode)]
  subs : Array[PipeSubCount]
}

///|
fn TrieNode::new() -> TrieNode {
  { edges: [], subs: [], }
}

///|
fn TrieNode::find_child_index(self : TrieNode, b : Byte) -> Int32 {
  for i = 0; i < self.edges.length(); i = i + 1 {
    let (edge_byte, _) = self.edges[i]
    if edge_byte == b {
      return i
    }
  }
  -1
}

///|
fn TrieNode::get_or_create_child(self : TrieNode, b : Byte) -> TrieNode {
  let idx = self.find_child_index(b)
  if idx >= 0 {
    let (_, child) = self.edges[idx]
    return child
  }
  let child = TrieNode::new()
  self.edges.push((b, child))
  child
}

///|
fn TrieNode::is_empty_branch(self : TrieNode) -> Bool {
  self.subs.is_empty() && self.edges.is_empty()
}

///|
/// Multi-pipe prefix subscription trie matching official `libzmq` `mtrie_t` semantics.
///
/// Tracks per-pipe reference counts on each byte-prefix topic so that:
/// - `add` returns `true` only when the first subscriber registers for `topic` (`0 -> 1`).
/// - `rm` returns `true` only when the last subscriber unregisters from `topic` (`1 -> 0`).
/// - `match_pipes` resolves all matching subscriber `pipe_id`s in `O(|payload|)` prefix walk.
pub struct SubscriptionTrie {
  priv root : TrieNode
  priv mut active_topic_count : Int32
  priv mut total_registrations : Int32
}

///|
/// Create a new empty `SubscriptionTrie`.
pub fn SubscriptionTrie::new() -> SubscriptionTrie {
  { root: TrieNode::new(), active_topic_count: 0, total_registrations: 0, }
}

///|
/// Number of distinct topics that currently have at least one subscribed pipe.
pub fn SubscriptionTrie::unique_topic_count(self : SubscriptionTrie) -> Int32 {
  self.active_topic_count
}

///|
/// Total active subscription registrations across all pipes and topics.
pub fn SubscriptionTrie::total_registrations(self : SubscriptionTrie) -> Int32 {
  self.total_registrations
}

///|
/// Register a subscription to `topic` for `pipe_id`.
///
/// Returns `true` if `topic` transitioned from 0 subscribers to 1 subscriber
/// (indicating an `XPUB` / `SUB` socket should forward `\x01<topic>` upstream).
pub fn SubscriptionTrie::add(
  self : SubscriptionTrie,
  topic : Bytes,
  pipe_id : Int32,
) -> Bool {
  let mut node = self.root
  for i = 0; i < topic.length(); i = i + 1 {
    node = node.get_or_create_child(topic[i])
  }
  let was_empty = node.subs.is_empty()
  let mut found = false
  for entry in node.subs {
    if entry.pipe_id == pipe_id {
      entry.count = entry.count + 1
      found = true
      break
    }
  }
  if !found {
    node.subs.push({ pipe_id, count: 1, })
  }
  self.total_registrations = self.total_registrations + 1
  if was_empty {
    self.active_topic_count = self.active_topic_count + 1
    true
  } else {
    false
  }
}

///|
fn remove_at_path(
  node : TrieNode,
  topic : Bytes,
  depth : Int32,
  pipe_id : Int32,
) -> (Bool, Bool) {
  if depth == topic.length() {
    let mut sub_idx = -1
    for i = 0; i < node.subs.length(); i = i + 1 {
      if node.subs[i].pipe_id == pipe_id {
        sub_idx = i
        break
      }
    }
    if sub_idx < 0 {
      return (false, false)
    }
    if node.subs[sub_idx].count > 1 {
      node.subs[sub_idx].count = node.subs[sub_idx].count - 1
      return (true, false)
    }
    let _ = node.subs.remove(sub_idx)
    let dropped_to_zero = node.subs.is_empty()
    return (true, dropped_to_zero)
  }
  let b = topic[depth]
  let child_idx = node.find_child_index(b)
  if child_idx < 0 {
    return (false, false)
  }
  let (_, child) = node.edges[child_idx]
  let (removed, dropped_to_zero) = remove_at_path(
    child,
    topic,
    depth + 1,
    pipe_id,
  )
  if removed && child.is_empty_branch() {
    let _ = node.edges.remove(child_idx)
  }
  (removed, dropped_to_zero)
}

///|
/// Unregister one subscription to `topic` for `pipe_id`.
///
/// Returns `true` if `topic` transitioned from 1 subscriber down to 0 subscribers
/// (indicating an `XPUB` / `SUB` socket should forward `\x00<topic>` upstream).
pub fn SubscriptionTrie::rm(
  self : SubscriptionTrie,
  topic : Bytes,
  pipe_id : Int32,
) -> Bool {
  let (removed, dropped_to_zero) = remove_at_path(self.root, topic, 0, pipe_id)
  if removed {
    self.total_registrations = self.total_registrations - 1
  }
  if dropped_to_zero {
    self.active_topic_count = self.active_topic_count - 1
  }
  dropped_to_zero
}

///|
fn remove_pipe_recursive(
  node : TrieNode,
  prefix_buf : Array[Byte],
  pipe_id : Int32,
  emptied_topics : Array[Bytes],
) -> Int32 {
  let mut removed_regs = 0
  let mut sub_idx = -1
  for i = 0; i < node.subs.length(); i = i + 1 {
    if node.subs[i].pipe_id == pipe_id {
      sub_idx = i
      break
    }
  }
  if sub_idx >= 0 {
    let entry = node.subs.remove(sub_idx)
    removed_regs = removed_regs + entry.count
    if node.subs.is_empty() {
      emptied_topics.push(Bytes::from_array(prefix_buf))
    }
  }
  let mut i = 0
  while i < node.edges.length() {
    let (edge_byte, child) = node.edges[i]
    prefix_buf.push(edge_byte)
    let child_removed = remove_pipe_recursive(
      child, prefix_buf, pipe_id, emptied_topics,
    )
    let _ = prefix_buf.pop()
    removed_regs = removed_regs + child_removed
    if child.is_empty_branch() {
      let _ = node.edges.remove(i)
    } else {
      i = i + 1
    }
  }
  removed_regs
}

///|
/// Remove all subscriptions owned by `pipe_id` (used when a peer disconnects).
/// Returns an array of all topics whose total subscriber count dropped to `0`.
pub fn SubscriptionTrie::remove_pipe(
  self : SubscriptionTrie,
  pipe_id : Int32,
) -> Array[Bytes] {
  let emptied : Array[Bytes] = []
  let prefix_buf : Array[Byte] = []
  let removed_regs = remove_pipe_recursive(
    self.root,
    prefix_buf,
    pipe_id,
    emptied,
  )
  self.total_registrations = self.total_registrations - removed_regs
  self.active_topic_count = self.active_topic_count - emptied.length()
  emptied
}

///|
fn append_unique_pipes(subs : Array[PipeSubCount], out : Array[Int32]) -> Unit {
  for entry in subs {
    let mut exists = false
    for existing in out {
      if existing == entry.pipe_id {
        exists = true
        break
      }
    }
    if !exists {
      out.push(entry.pipe_id)
    }
  }
}

///|
/// Return all unique `pipe_id`s whose subscribed prefix matches `payload`.
///
/// Runs in `O(|payload|)` trie depth steps without scanning unrelated topic branches.
pub fn SubscriptionTrie::match_pipes(
  self : SubscriptionTrie,
  payload : Bytes,
) -> Array[Int32] {
  let matched : Array[Int32] = []
  let mut node = self.root
  append_unique_pipes(node.subs, matched)
  for i = 0; i < payload.length(); i = i + 1 {
    let idx = node.find_child_index(payload[i])
    if idx < 0 {
      break
    }
    let (_, next_node) = node.edges[idx]
    node = next_node
    append_unique_pipes(node.subs, matched)
  }
  matched
}

///|
/// Fast boolean check returning `true` if at least one registered prefix matches `payload`.
pub fn SubscriptionTrie::matches_any(
  self : SubscriptionTrie,
  payload : Bytes,
) -> Bool {
  let mut node = self.root
  if !node.subs.is_empty() {
    return true
  }
  for i = 0; i < payload.length(); i = i + 1 {
    let idx = node.find_child_index(payload[i])
    if idx < 0 {
      return false
    }
    let (_, next_node) = node.edges[idx]
    node = next_node
    if !node.subs.is_empty() {
      return true
    }
  }
  false
}

///|
/// Return the number of distinct pipes currently subscribed to exact `topic`.
pub fn SubscriptionTrie::subscriber_count(
  self : SubscriptionTrie,
  topic : Bytes,
) -> Int32 {
  let mut node = self.root
  for i = 0; i < topic.length(); i = i + 1 {
    let idx = node.find_child_index(topic[i])
    if idx < 0 {
      return 0
    }
    let (_, next_node) = node.edges[idx]
    node = next_node
  }
  node.subs.length()
}

///|
fn collect_topics_recursive(
  node : TrieNode,
  prefix_buf : Array[Byte],
  filter_pipe : Int32?,
  out : Array[Bytes],
) -> Unit {
  let include_here = match filter_pipe {
    None => !node.subs.is_empty()
    Some(pid) => {
      let mut has_pid = false
      for s in node.subs {
        if s.pipe_id == pid {
          has_pid = true
          break
        }
      }
      has_pid
    }
  }
  if include_here {
    out.push(Bytes::from_array(prefix_buf))
  }
  for edge in node.edges {
    let (b, child) = edge
    prefix_buf.push(b)
    collect_topics_recursive(child, prefix_buf, filter_pipe, out)
    let _ = prefix_buf.pop()
  }
}

///|
/// List all currently active subscription topics in this trie.
pub fn SubscriptionTrie::all_topics(self : SubscriptionTrie) -> Array[Bytes] {
  let out : Array[Bytes] = []
  let buf : Array[Byte] = []
  collect_topics_recursive(self.root, buf, None, out)
  out
}

///|
/// List all active topics subscribed by a specific `pipe_id`.
pub fn SubscriptionTrie::topics_for_pipe(
  self : SubscriptionTrie,
  pipe_id : Int32,
) -> Array[Bytes] {
  let out : Array[Bytes] = []
  let buf : Array[Byte] = []
  collect_topics_recursive(self.root, buf, Some(pipe_id), out)
  out
}

///|
/// Clear all subscriptions from the trie.
pub fn SubscriptionTrie::clear(self : SubscriptionTrie) -> Unit {
  self.root.edges.clear()
  self.root.subs.clear()
  self.active_topic_count = 0
  self.total_registrations = 0
}

///|
/// Cached topic snapshot stored inside a `LastValueCache`.
pub struct LvcEntry {
  topic : Bytes
  message : Message
  updated_at_ms : Int64
  sequence : Int64
} derive(Eq, Debug)

///|
pub extend LvcEntry with Eq::{not_equal, equal}

///|
pub extend LvcEntry with @debug.Debug::{to_repr}

///|
/// Last-Value Cache (LVC) engine for ZeroMQ PUB/SUB & XPUB/XSUB brokers
/// (ZeroMQ Guide Chapter 5 "Last Value Caching").
///
/// Retains the most recent `Message` published for each topic, supports optional TTL
/// expiration and bounded capacity eviction, and replays matching cached messages
/// whenever a late-joining subscriber subscribes to a topic prefix.
pub struct LastValueCache {
  max_topics : Int32
  ttl_ms : Int64
  entries : Array[LvcEntry]
  mut next_seq : Int64
  mut cache_hits : Int64
  mut cache_updates : Int64
  mut evictions : Int64
}

///|
/// Create a new `LastValueCache` with bounded `max_topics` and optional `ttl_ms`
/// (`0L` disables time-based expiration).
pub fn LastValueCache::new(
  max_topics~ : Int32 = 4096,
  ttl_ms~ : Int64 = 0L,
) -> LastValueCache {
  let cap = if max_topics <= 0 { 1 } else { max_topics }
  let effective_ttl = if ttl_ms < 0L { 0L } else { ttl_ms }
  {
    max_topics: cap,
    ttl_ms: effective_ttl,
    entries: [],
    next_seq: 1L,
    cache_hits: 0L,
    cache_updates: 0L,
    evictions: 0L,
  }
}

///|
/// Number of topics currently stored in the cache.
pub fn LastValueCache::size(self : LastValueCache) -> Int32 {
  self.entries.length()
}

///|
/// Total number of topic updates recorded in the cache.
pub fn LastValueCache::updates_count(self : LastValueCache) -> Int64 {
  self.cache_updates
}

///|
/// Total number of prefix/exact replay hits served from the cache.
pub fn LastValueCache::hits_count(self : LastValueCache) -> Int64 {
  self.cache_hits
}

///|
/// Total number of entries evicted due to capacity limits or TTL expiration.
pub fn LastValueCache::evictions_count(self : LastValueCache) -> Int64 {
  self.evictions
}

///|
fn LastValueCache::is_entry_expired(
  self : LastValueCache,
  entry : LvcEntry,
  now_ms : Int64,
) -> Bool {
  if self.ttl_ms <= 0L {
    return false
  }
  now_ms - entry.updated_at_ms > self.ttl_ms
}

///|
/// Remove all expired entries as of `now_ms` and return the count of purged entries.
pub fn LastValueCache::purge_expired(
  self : LastValueCache,
  now_ms : Int64,
) -> Int32 {
  if self.ttl_ms <= 0L {
    return 0
  }
  let mut purged = 0
  let mut i = 0
  while i < self.entries.length() {
    if self.is_entry_expired(self.entries[i], now_ms) {
      let _ = self.entries.remove(i)
      purged = purged + 1
      self.evictions = self.evictions + 1L
    } else {
      i = i + 1
    }
  }
  purged
}

///|
/// Store or overwrite the latest `message` for `topic` at timestamp `now_ms`.
/// If `max_topics` is exceeded, evicts the oldest updated topic.
pub fn LastValueCache::store(
  self : LastValueCache,
  topic : Bytes,
  message : Message,
  now_ms~ : Int64 = 0L,
) -> Unit {
  let seq = self.next_seq
  self.next_seq = self.next_seq + 1L
  self.cache_updates = self.cache_updates + 1L
  for i = 0; i < self.entries.length(); i = i + 1 {
    if self.entries[i].topic == topic {
      self.entries[i] = {
        topic,
        message,
        updated_at_ms: now_ms,
        sequence: seq,
      }
      return
    }
  }
  if self.entries.length() >= self.max_topics {
    let _ = self.purge_expired(now_ms)
  }
  if self.entries.length() >= self.max_topics {
    let mut oldest_idx = 0
    let mut oldest_seq = self.entries[0].sequence
    for i = 1; i < self.entries.length(); i = i + 1 {
      if self.entries[i].sequence < oldest_seq {
        oldest_seq = self.entries[i].sequence
        oldest_idx = i
      }
    }
    let _ = self.entries.remove(oldest_idx)
    self.evictions = self.evictions + 1L
  }
  self.entries.push({ topic, message, updated_at_ms: now_ms, sequence: seq, })
}

///|
/// Store a message using its first frame as the canonical topic key (or full single-frame
/// prefix up to `delimiter` byte if single-frame `"topic payload"` style is used).
pub fn LastValueCache::store_auto_topic(
  self : LastValueCache,
  msg : Message,
  delimiter~ : Byte? = None,
  now_ms~ : Int64 = 0L,
) -> Bytes {
  if msg.frames.is_empty() {
    self.store(b"", msg, now_ms~)
    return b""
  }
  let first = msg.frames[0]
  let topic = if msg.frames.length() >= 2 {
    first
  } else {
    match delimiter {
      Some(delim) => {
        let mut split_at = -1
        for i = 0; i < first.length(); i = i + 1 {
          if first[i] == delim {
            split_at = i
            break
          }
        }
        if split_at >= 0 {
          slice_bytes(first, 0, split_at)
        } else {
          first
        }
      }
      None => first
    }
  }
  self.store(topic, msg, now_ms~)
  topic
}

///|
/// Retrieve the cached message for exact `topic`, if present and not expired.
pub fn LastValueCache::get_exact(
  self : LastValueCache,
  topic : Bytes,
  now_ms~ : Int64 = 0L,
) -> Message? {
  for entry in self.entries {
    if entry.topic == topic {
      if self.is_entry_expired(entry, now_ms) {
        return None
      }
      self.cache_hits = self.cache_hits + 1L
      return Some(entry.message)
    }
  }
  None
}

///|
fn bytes_has_prefix(target : Bytes, prefix : Bytes) -> Bool {
  if prefix.length() > target.length() {
    return false
  }
  for i = 0; i < prefix.length(); i = i + 1 {
    if target[i] != prefix[i] {
      return false
    }
  }
  true
}

///|
/// Query all non-expired cached messages whose topic starts with `prefix`.
/// Used by an `XPUB` LVC broker to immediately replay cached values to new subscribers.
pub fn LastValueCache::query_prefix(
  self : LastValueCache,
  prefix : Bytes,
  now_ms~ : Int64 = 0L,
) -> Array[Message] {
  let matched : Array[Message] = []
  for entry in self.entries {
    if !self.is_entry_expired(entry, now_ms) &&
      bytes_has_prefix(entry.topic, prefix) {
      matched.push(entry.message)
      self.cache_hits = self.cache_hits + 1L
    }
  }
  matched
}

///|
/// Clear all cached topic entries.
pub fn LastValueCache::clear(self : LastValueCache) -> Unit {
  self.entries.clear()
}

///|
/// Run an asynchronous Last-Value Caching (LVC) broker bridging an `XSub` `frontend`
/// (connected to upstream publishers) and an `XPub` `backend` (serving downstream subscribers),
/// controlled by an optional steerable `control` socket (`"TERMINATE"` stops the loop).
pub async fn run_lvc_proxy(
  frontend : Socket,
  backend : Socket,
  cache : LastValueCache,
  control~ : Socket? = None,
  topic_delimiter~ : Byte? = Some(b' '),
) -> Int64 raise ZmqError {
  backend.set_xpub_verbose(true)
  let poller = Poller::new()
  poller.add(frontend, POLLIN)
  poller.add(backend, POLLIN)
  match control {
    Some(ctrl) => poller.add(ctrl, POLLIN)
    None => ()
  }
  let mut replayed : Int64 = 0L
  while !frontend.is_closed() && !backend.is_closed() {
    let ready = poller.poll(timeout_ms=25) catch { _ => raise Closed }
    if ready <= 0 {
      continue
    }
    match control {
      Some(ctrl) =>
        if poller.has_in(ctrl) {
          let cmd_msg = ctrl.recv()
          if cmd_msg.first_string() == "TERMINATE" {
            return replayed
          }
        }
      None => ()
    }
    if poller.has_in(frontend) {
      let pub_msg = frontend.recv()
      let now_ms = @async.now()
      let _ = cache.store_auto_topic(
        pub_msg,
        delimiter=topic_delimiter,
        now_ms~,
      )
      // Publish downstream through XPUB backend pipes
      backend.hub.send_pub(pub_msg) catch {
        _ => ()
      }
    }
    if poller.has_in(backend) {
      let sub_msg = backend.recv()
      // Forward subscription/unsubscription frame upstream to XSUB
      frontend.send(sub_msg)
      match decode_subscription_frame(sub_msg) {
        Some((true, prefix)) => {
          let hits = cache.query_prefix(prefix, now_ms=@async.now())
          for cached_msg in hits {
            backend.hub.send_pub(cached_msg) catch {
              _ => ()
            }
            replayed = replayed + 1L
          }
        }
        _ => ()
      }
    }
  }
  replayed
}

///|
/// Conflating message buffer implementing ZeroMQ `ZMQ_CONFLATE` behavior.
///
/// Instead of queueing unbounded historical messages during producer bursts,
/// `ConflateBuffer` overwrites stale unread messages so consumers always read
/// the freshest state (either globally when `by_topic=false` or per topic frame when `by_topic=true`).
pub struct ConflateBuffer {
  by_topic : Bool
  mut single_latest : Message?
  topic_slots : Array[(Bytes, Message)]
  mut pushed_total : Int64
  mut overwritten_total : Int64
  notify : @aqueue.Queue[Int32]
}

///|
/// Create a new `ConflateBuffer`.
/// - `by_topic=false`: classic `ZMQ_CONFLATE` single-slot buffer keeping only the last message.
/// - `by_topic=true`: multi-topic conflation keeping the latest message per first-frame topic.
pub fn ConflateBuffer::new(by_topic~ : Bool = false) -> ConflateBuffer {
  {
    by_topic,
    single_latest: None,
    topic_slots: [],
    pushed_total: 0L,
    overwritten_total: 0L,
    notify: @aqueue.Queue(kind=Unbounded),
  }
}

///|
/// Push a new `Message` into the conflating buffer, replacing any unread stale value.
pub fn ConflateBuffer::push(self : ConflateBuffer, msg : Message) -> Unit {
  self.pushed_total = self.pushed_total + 1L
  if !self.by_topic {
    if self.single_latest != None {
      self.overwritten_total = self.overwritten_total + 1L
    }
    self.single_latest = Some(msg)
  } else {
    let key = msg.first_bytes()
    let mut updated = false
    for i = 0; i < self.topic_slots.length(); i = i + 1 {
      let (existing_key, _) = self.topic_slots[i]
      if existing_key == key {
        self.topic_slots[i] = (key, msg)
        self.overwritten_total = self.overwritten_total + 1L
        updated = true
        break
      }
    }
    if !updated {
      self.topic_slots.push((key, msg))
    }
  }
  let _ = self.notify.try_put(1) catch { _ => false }
}

///|
/// Whether at least one conflated message is ready to pop without blocking.
pub fn ConflateBuffer::has_message(self : ConflateBuffer) -> Bool {
  if !self.by_topic {
    self.single_latest != None
  } else {
    !self.topic_slots.is_empty()
  }
}

///|
/// Non-blockingly pop the latest message (or earliest-queued dirty topic in `by_topic` mode).
pub fn ConflateBuffer::try_pop(self : ConflateBuffer) -> Message? {
  if !self.by_topic {
    let current = self.single_latest
    self.single_latest = None
    current
  } else if self.topic_slots.is_empty() {
    None
  } else {
    let (_, msg) = self.topic_slots.remove(0)
    Some(msg)
  }
}

///|
/// Asynchronously wait for and pop the latest conflated message.
pub async fn ConflateBuffer::pop(
  self : ConflateBuffer,
) -> Message raise ZmqError {
  while true {
    match self.try_pop() {
      Some(msg) => return msg
      None => ()
    }
    let _ = self.notify.get() catch { _ => raise Closed }
  }
  raise Closed
}

///|
/// Total number of messages pushed into this `ConflateBuffer`.
pub fn ConflateBuffer::pushed_count(self : ConflateBuffer) -> Int64 {
  self.pushed_total
}

///|
/// Total number of stale unread messages coalesced/overwritten before consumption.
pub fn ConflateBuffer::overwritten_count(self : ConflateBuffer) -> Int64 {
  self.overwritten_total
}
