///|
/// ZeroMQ Poller Multiplexing Example in MoonBit
/// Demonstrates non-blocking I/O event multiplexing across high-priority control
/// channels and high-throughput data streams using `Poller`.
async fn main {
  println("=== ZeroMQ Poller Multiplexing Example in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Channel 1: High-priority Urgent Command Channel (PAIR)
    let cmd_server = ctx.socket(@moon_zmq.Pair) catch { _ => return }
    let cmd_ep = cmd_server.bind(group, "tcp://127.0.0.1:0") catch {
      _ => return
    }

    let cmd_client = ctx.socket(@moon_zmq.Pair) catch { _ => return }
    cmd_client.connect(group, cmd_ep)

    // 2. Channel 2: High-throughput Data Stream Channel (PULL)
    let data_server = ctx.socket(@moon_zmq.Pull) catch { _ => return }
    let data_ep = data_server.bind(group, "tcp://127.0.0.1:0") catch {
      _ => return
    }

    let data_client = ctx.socket(@moon_zmq.Push) catch { _ => return }
    data_client.connect(group, data_ep)

    while cmd_server.active_peer_count() < 1 ||
          cmd_client.active_peer_count() < 1 ||
          data_server.active_peer_count() < 1 ||
          data_client.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // 3. Register sockets with Poller
    let poller = @moon_zmq.Poller::new()
    poller.add(cmd_server, @moon_zmq.POLLIN)
    poller.add(data_server, @moon_zmq.POLLIN)
    println(
      "[Poller] Monitored sockets registered: \{poller.length()} channels",
    )

    // Send mixed traffic
    let _ = data_client.send(
      @moon_zmq.Message::from_string("telemetry_batch_01"),
    ) catch {
      _ => return
    }
    let _ = cmd_client.send(
      @moon_zmq.Message::from_string("URGENT_EMERGENCY_STOP"),
    ) catch {
      _ => return
    }
    let _ = data_client.send(
      @moon_zmq.Message::from_string("telemetry_batch_02"),
    ) catch {
      _ => return
    }

    // Multiplex loop
    let mut handled_cmd = false
    let mut handled_data = 0

    while !handled_cmd || handled_data < 2 {
      let rc = poller.poll(timeout_ms=100) catch { _ => 0 }
      if rc <= 0 {
        continue
      }

      // Priority 1: Check command channel first
      if poller.has_in(cmd_server) {
        let msg = cmd_server.recv() catch { _ => break }
        println(
          "[Poller Dispatched] Priority CMD Channel: '\{msg.first_string()}'",
        )
        handled_cmd = true
      }

      // Priority 2: Check data channel
      if poller.has_in(data_server) {
        let msg = data_server.recv() catch { _ => break }
        println(
          "[Poller Dispatched] Regular DATA Channel: '\{msg.first_string()}'",
        )
        handled_data = handled_data + 1
      }
    }

    println("=== Poller Multiplexing Example Completed Successfully ===")
    cmd_client.close()
    cmd_server.close()
    data_client.close()
    data_server.close()
    ctx.term()
  })
}
