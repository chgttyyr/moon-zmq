///|
/// ZeroMQ `Poller` multiplexer implementation for asynchronous event-driven I/O.
/// Allows monitoring multiple `Socket` instances simultaneously for readability (`POLLIN`),
/// writability (`POLLOUT`), and error states (`POLLERR`).

///|
/// Event flag indicating that a message is ready to be received without blocking.
pub const POLLIN : Int32 = 0x01

///|
/// Event flag indicating that a message can be sent without blocking.
pub const POLLOUT : Int32 = 0x02

///|
/// Event flag indicating that an error condition occurred on the socket.
pub const POLLERR : Int32 = 0x04

///|
/// An item registered within a `Poller` tracking a specific `Socket` and interest bitmask.
pub struct PollItem {
  socket : Socket
  mut events : Int32
  mut revents : Int32
}

///|
/// Multiplexer monitoring multiple `Socket` instances for I/O readiness.
pub struct Poller {
  items : Array[PollItem]
}

///|
/// Create a new empty `Poller`.
pub fn Poller::new() -> Poller {
  { items: [], }
}

///|
/// Register a `Socket` to monitor for the specified `events` bitmask.
/// If already registered, updates the monitored event bitmask.
pub fn Poller::add(self : Poller, socket : Socket, events : Int32) -> Unit {
  for item in self.items {
    if item.socket.id == socket.id {
      item.events = events
      return
    }
  }
  self.items.push({ socket, events, revents: 0, })
}

///|
/// Modify the interest bitmask for an already registered `Socket`.
/// Returns `true` if modified, `false` if the socket was not registered.
pub fn Poller::modify(self : Poller, socket : Socket, events : Int32) -> Bool {
  for item in self.items {
    if item.socket.id == socket.id {
      item.events = events
      return true
    }
  }
  false
}

///|
/// Unregister a `Socket` from this `Poller`.
/// Returns `true` if found and removed, `false` otherwise.
pub fn Poller::remove(self : Poller, socket : Socket) -> Bool {
  for i = 0; i < self.items.length(); i = i + 1 {
    if self.items[i].socket.id == socket.id {
      let _ = self.items.remove(i)
      return true
    }
  }
  false
}

///|
/// Number of sockets currently monitored by this `Poller`.
pub fn Poller::length(self : Poller) -> Int32 {
  self.items.length()
}

///|
/// Clear all registered sockets from this `Poller`.
pub fn Poller::clear(self : Poller) -> Unit {
  self.items.clear()
}

///|
/// Internal check across all items. Sets `revents` on each item and returns
/// the count of items with non-zero ready events.
fn Poller::check_readiness(self : Poller) -> Int32 {
  let mut ready_count = 0
  for item in self.items {
    let r = item.socket.has_events(item.events)
    item.revents = r
    if r != 0 {
      ready_count = ready_count + 1
    }
  }
  ready_count
}

///|
/// Wait asynchronously for events on any registered `Socket`.
/// - `timeout_ms = 0`: Non-blocking immediate check.
/// - `timeout_ms < 0`: Wait indefinitely until at least one event is ready.
/// - `timeout_ms > 0`: Wait up to `timeout_ms` milliseconds.
/// Returns the number of ready sockets.
pub async fn Poller::poll(self : Poller, timeout_ms~ : Int32 = -1) -> Int32 {
  if self.items.is_empty() {
    return 0
  }

  // Fast-path: immediate check
  let ready = self.check_readiness()
  if ready > 0 || timeout_ms == 0 {
    return ready
  }

  if timeout_ms < 0 {
    while true {
      @async.sleep(5)
      let count = self.check_readiness()
      if count > 0 {
        return count
      }
    }
    0
  } else {
    let interval = 5
    let mut remaining = timeout_ms
    while remaining > 0 {
      let sleep_step = if remaining < interval { remaining } else { interval }
      @async.sleep(sleep_step)
      remaining = remaining - sleep_step
      let count = self.check_readiness()
      if count > 0 {
        return count
      }
    }
    // Final check on timeout expiry
    self.check_readiness()
  }
}

///|
/// Get the ready events bitmask (`revents`) computed during the last `poll` call for `socket`.
pub fn Poller::revents_for(self : Poller, socket : Socket) -> Int32 {
  for item in self.items {
    if item.socket.id == socket.id {
      return item.revents
    }
  }
  0
}

///|
/// Check whether `POLLIN` was signaled for `socket` in the last `poll` call.
pub fn Poller::has_in(self : Poller, socket : Socket) -> Bool {
  (self.revents_for(socket) & POLLIN) != 0
}

///|
/// Check whether `POLLOUT` was signaled for `socket` in the last `poll` call.
pub fn Poller::has_out(self : Poller, socket : Socket) -> Bool {
  (self.revents_for(socket) & POLLOUT) != 0
}

///|
/// Check whether `POLLERR` was signaled for `socket` in the last `poll` call.
pub fn Poller::has_err(self : Poller, socket : Socket) -> Bool {
  (self.revents_for(socket) & POLLERR) != 0
}
