///|
/// Request-Reply (REQ/REP) Example in MoonBit
/// Demonstrates synchronized request-response exchange with automatic envelope handling.

async fn main {
  println("=== ZeroMQ REQ/REP Example in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Initialize REP Server
    let server = ctx.socket(@moon_zmq.Rep) catch {
      err => {
        println("Failed to create REP socket: \{err}")
        return
      }
    }
    let endpoint = server.bind(group, "tcp://127.0.0.1:0") catch {
      err => {
        println("Failed to bind server: \{err}")
        return
      }
    }
    println("[Server] Bound to \{endpoint}")

    // 2. Initialize REQ Client
    let client = ctx.socket(@moon_zmq.Req) catch {
      err => {
        println("Failed to create REQ socket: \{err}")
        return
      }
    }
    client.connect(group, endpoint)
    println("[Client] Connecting to \{endpoint}...")

    // 3. Await peer connection
    while server.active_peer_count() < 1 || client.active_peer_count() < 1 {
      @async.sleep(10)
    }
    println("[Client] Connected to server successfully.")

    // 4. Send Request 1
    println("[Client] Sending request: 'ping'")
    let _ = client.send(@moon_zmq.Message::from_string("ping")) catch {
      err => {
        println("Send failed: \{err}")
        return
      }
    }

    let req = server.recv() catch {
      err => {
        println("Server recv failed: \{err}")
        return
      }
    }
    println("[Server] Received request: '\{req.first_string()}'")

    println("[Server] Sending response: 'pong'")
    let _ = server.send(@moon_zmq.Message::from_string("pong")) catch {
      err => {
        println("Server send failed: \{err}")
        return
      }
    }

    let rep = client.recv() catch {
      err => {
        println("Client recv failed: \{err}")
        return
      }
    }
    println("[Client] Received response: '\{rep.first_string()}'")

    // 5. Clean teardown
    client.close()
    server.close()
    ctx.term()
    println("=== REQ/REP Completed Successfully ===")
  })
}
