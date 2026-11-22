///|
/// Socket Lifecycle Monitoring & Observability Example in MoonBit
/// Demonstrates real-time connection event capture (Listening, Accepted, HandshakeSucceeded,
/// Disconnected, Closed) and traffic metrics tracking (`messages_sent`, `bytes_sent`).
async fn main {
  println("=== ZeroMQ Socket Lifecycle & Monitoring Example in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Setup Server with Event Monitor
    let server = ctx.socket(@moon_zmq.Router) catch { _ => return }
    let srv_monitor = server.monitor_events()

    // Background monitor logger
    let monitor_task = group.spawn(no_wait=true, allow_failure=true, async fn() {
      while !server.is_closed() {
        let ev = srv_monitor.get() catch { _ => break }
        match ev {
          Listening(ep) => println("[Monitor Event] Server listening on \{ep}")
          Connected(id) =>
            println("[Monitor Event] New peer connected: pipe_id=\{id}")
          HandshakeSucceeded(peer_type, ident) =>
            println(
              "[Monitor Event] Handshake completed: peer_type=\{peer_type}, identity='\{@moon_zmq.ascii_bytes_to_string(ident)}'",
            )
          Disconnected(id) =>
            println("[Monitor Event] Peer disconnected: pipe_id=\{id}")
          Closed => println("[Monitor Event] Socket closed")
          _ => ()
        }
      }
    })

    let endpoint = server.bind(group, "tcp://127.0.0.1:0") catch { _ => return }

    // 2. Setup Client
    let client = ctx.socket(@moon_zmq.Dealer) catch { _ => return }
    client.set_identity(b"client-telemetry")
    client.connect(group, endpoint)

    while client.active_peer_count() < 1 || server.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // 3. Send batch messages and measure metrics
    let batch = [
      @moon_zmq.Message::from_string("telemetry_metric_cpu_load=45.2"),
      @moon_zmq.Message::from_string("telemetry_metric_mem_used=1024MB"),
      @moon_zmq.Message::from_string("telemetry_metric_disk_io=120IOPS"),
    ]

    let count = client.send_batch(batch)
    println("[Client] Batch sent: \{count} telemetry messages")

    // Server receives batch
    @async.sleep(50)
    let received_batch = server.recv_batch(max_count=10)
    println("[Server] Batch drained: \{received_batch.length()} messages")
    for msg in received_batch {
      let text = msg.frame_string_at(1).unwrap_or("")
      println("  -> Payload: \{text}")
    }

    // 4. Print socket metrics
    println("--- Live Socket Traffic Metrics ---")
    println(
      "[Client Metrics] msgs_sent=\{client.messages_sent()}, bytes_sent=\{client.bytes_sent()}",
    )
    println(
      "[Server Metrics] msgs_recv=\{server.messages_received()}, bytes_recv=\{server.bytes_received()}",
    )

    // 5. Client disconnects
    client.close()
    @async.sleep(50)

    println("=== Socket Monitoring Example Completed Successfully ===")
    monitor_task.cancel()
    server.close()
    ctx.term()
  })
}
