///|
/// RFC 43 Steerable Proxy Broker Example in MoonBit
/// Demonstrates automatic message forwarding and traffic capture connecting
/// REQ clients and REP workers through a ROUTER <-> DEALER proxy.
async fn main {
  println("=== ZeroMQ RFC 43 Proxy Broker with Capture in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Setup Proxy frontend (ROUTER) and backend (DEALER)
    let frontend = ctx.socket(@moon_zmq.Router) catch { _ => return }
    let f_ep = frontend.bind(group, "tcp://127.0.0.1:0") catch { _ => return }
    println("[Proxy] Frontend (ROUTER) listening on \{f_ep}")

    let backend = ctx.socket(@moon_zmq.Dealer) catch { _ => return }
    let b_ep = backend.bind(group, "tcp://127.0.0.1:0") catch { _ => return }
    println("[Proxy] Backend (DEALER) listening on \{b_ep}")

    // 2. Setup Audit Capture sink (PULL)
    let capture_sink = ctx.socket(@moon_zmq.Pull) catch { _ => return }
    let c_ep = capture_sink.bind(group, "tcp://127.0.0.1:0") catch {
      _ => return
    }

    let capture_push = ctx.socket(@moon_zmq.Push) catch { _ => return }
    capture_push.connect(group, c_ep)

    // 3. Spawn Proxy forwarder task
    let stats = @moon_zmq.ProxyStats::new()
    let proxy_task = group.spawn(no_wait=true, allow_failure=true, async fn() {
      let _ = @moon_zmq.proxy_steerable(
        frontend,
        backend,
        capture=Some(capture_push),
        stats~,
      ) catch {
        _ => ()
      }
    })

    // 4. Start REP Worker connected to Proxy Backend
    let worker = ctx.socket(@moon_zmq.Rep) catch { _ => return }
    worker.connect(group, b_ep)

    let worker_task = group.spawn(no_wait=true, allow_failure=true, async fn() {
      while !worker.is_closed() {
        let req = worker.recv() catch { _ => break }
        let text = req.first_string()
        println("[Worker] Received request via proxy: '\{text}'")
        let _ = worker.send(@moon_zmq.Message::from_string("PROCESSED:\{text}")) catch {
          _ => break
        }
      }
    })

    // 5. Start REQ Client connected to Proxy Frontend
    let client = ctx.socket(@moon_zmq.Req) catch { _ => return }
    client.connect(group, f_ep)

    while frontend.active_peer_count() < 1 ||
          backend.active_peer_count() < 1 ||
          client.active_peer_count() < 1 ||
          worker.active_peer_count() < 1 ||
          capture_push.active_peer_count() < 1 ||
          capture_sink.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Client sends two requests
    for i in 1..<=2 {
      let query = "task_#\{i}"
      println("[Client] Sending request: '\{query}'")
      let _ = client.send(@moon_zmq.Message::from_string(query)) catch {
        _ => return
      }

      let reply = client.recv() catch { _ => return }
      println("[Client] Received reply: '\{reply.first_string()}'")

      // Verify captured traffic
      let cap_req = capture_sink.recv() catch { _ => return }
      let cap_rep = capture_sink.recv() catch { _ => return }
      let last_frame = cap_req.frames[cap_req.frames.length() - 1]
      println(
        "[Capture] Audited message: '\{@moon_zmq.ascii_bytes_to_string(last_frame)}' (frames=\{cap_req.frames.length()})",
      )
      let _ = cap_rep
    }

    println("=== Proxy Broker Example Completed Successfully ===")
    proxy_task.cancel()
    worker_task.cancel()
    client.close()
    worker.close()
    capture_push.close()
    capture_sink.close()
    frontend.close()
    backend.close()
    ctx.term()
  })
}
