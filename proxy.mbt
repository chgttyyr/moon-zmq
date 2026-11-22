///|
/// RFC ZeroMQ Proxy and Steerable Proxy implementation (RFC 43).
///
/// A proxy forwards messages bidirectionally between a `frontend` and a `backend` socket.
/// Supports optional message capture for monitoring/auditing, as well as dynamic runtime
/// control (PAUSE, RESUME, TERMINATE, STATISTICS) via a `control` socket.

///|
/// Runtime operational state of a proxy forwarder.
pub(all) enum ProxyState {
  Active
  Paused
  Terminated
} derive(Eq, Debug)

///|
pub extend ProxyState with Eq::{not_equal, equal}

///|
pub extend ProxyState with @debug.Debug::{to_repr}

///|
/// Performance and message counters tracked by a proxy instance.
pub struct ProxyStats {
  mut frontend_messages : Int64
  mut backend_messages : Int64
  mut captured_messages : Int64
}

///|
/// Create a new `ProxyStats` counter initialized to zero.
pub fn ProxyStats::new() -> ProxyStats {
  { frontend_messages: 0, backend_messages: 0, captured_messages: 0, }
}

///|
/// High-level ZeroMQ proxy connecting `frontend` and `backend` sockets with optional `capture`.
/// Forwards messages from `frontend` to `backend` and vice versa until one of the sockets is closed.
pub async fn proxy(
  frontend : Socket,
  backend : Socket,
  capture~ : Socket? = None,
) -> Unit {
  proxy_steerable(frontend, backend, capture~, control=None)
}

///|
/// RFC 43 Steerable Proxy supporting runtime control commands via `control` socket.
/// Recognized control commands:
/// - `"PAUSE"`: Suspends message forwarding between frontend and backend.
/// - `"RESUME"`: Resumes message forwarding.
/// - `"TERMINATE"`: Gracefully terminates the proxy forwarder.
pub async fn proxy_steerable(
  frontend : Socket,
  backend : Socket,
  capture~ : Socket? = None,
  control~ : Socket? = None,
  stats~ : ProxyStats = ProxyStats::new(),
) -> Unit {
  let poller = Poller::new()
  poller.add(frontend, POLLIN)
  poller.add(backend, POLLIN)

  match control {
    Some(ctrl) => poller.add(ctrl, POLLIN)
    None => ()
  }

  let mut state = ProxyState::Active

  while !frontend.is_closed() &&
        !backend.is_closed() &&
        !@async.is_being_cancelled() {
    let rc = poller.poll(timeout_ms=20) catch { _ => 0 }
    if rc <= 0 {
      continue
    }

    // 1. Process control socket messages first
    match control {
      Some(ctrl) =>
        if poller.has_in(ctrl) {
          let cmd_msg = ctrl.recv() catch { _ => break }
          let cmd = cmd_msg.first_string()
          if cmd == "PAUSE" {
            state = Paused
            let _ = poller.modify(frontend, 0)
            let _ = poller.modify(backend, 0)
          } else if cmd == "RESUME" {
            state = Active
            let _ = poller.modify(frontend, POLLIN)
            let _ = poller.modify(backend, POLLIN)
          } else if cmd == "TERMINATE" {
            state = Terminated
            break
          }
        }
      None => ()
    }

    if state == Terminated {
      break
    }

    // 2. When active, forward messages between frontend and backend
    if state == Active {
      // Frontend -> Backend
      if poller.has_in(frontend) {
        let msg = frontend.recv() catch { _ => break }
        stats.frontend_messages = stats.frontend_messages + 1

        match capture {
          Some(cap) => {
            let _ = cap.send(msg) catch { _ => () }
            stats.captured_messages = stats.captured_messages + 1
          }
          None => ()
        }

        backend.send(msg) catch {
          _ => break
        }
      }

      // Backend -> Frontend
      if poller.has_in(backend) {
        let msg = backend.recv() catch { _ => break }
        stats.backend_messages = stats.backend_messages + 1

        match capture {
          Some(cap) => {
            let _ = cap.send(msg) catch { _ => () }
            stats.captured_messages = stats.captured_messages + 1
          }
          None => ()
        }

        frontend.send(msg) catch {
          _ => break
        }
      }
    }
  }
}
