///|
/// Asynchronous Message Broker (ROUTER / DEALER) Example in MoonBit
/// Demonstrates non-blocking identity routing between heterogeneous clients and backend workers.

async fn main {
  println("=== ZeroMQ ROUTER / DEALER Message Broker Example in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Central ROUTER Broker
    let router = ctx.socket(@moon_zmq.Router) catch {
      err => {
        println("Failed to create ROUTER: \{err}")
        return
      }
    }
    let endpoint = router.bind(group, "tcp://127.0.0.1:0") catch {
      err => {
        println("Failed to bind ROUTER: \{err}")
        return
      }
    }
    println("[Broker] Router listening on \{endpoint}")

    // 2. Asynchronous DEALER Client
    let client = ctx.socket(@moon_zmq.Dealer) catch {
      err => {
        println("Failed to create DEALER client: \{err}")
        return
      }
    }
    client.set_identity(b"client-alpha")
    client.connect(group, endpoint)

    while router.active_peer_count() < 1 || client.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // 3. Client sends request through dealer (empty delimiter + payload)
    println("[Client-Alpha] Sending async RPC request: 'query_user_profile(id=1001)'")
    let client_req : Array[Bytes] = [
      b"",
      b"query_user_profile(id=1001)",
    ]
    let _ = client.send(@moon_zmq.Message::multipart(client_req)) catch {
      err => {
        println("Client send error: \{err}")
        return
      }
    }

    // 4. Broker receives message: [peer_id, empty_delim, payload]
    let broker_msg = router.recv() catch {
      err => {
        println("Broker recv error: \{err}")
        return
      }
    }
    let sender_id = @moon_zmq.ascii_bytes_to_string(broker_msg.frames[0])
    let req_payload = @moon_zmq.ascii_bytes_to_string(broker_msg.frames[2])
    println("[Broker] Received from [\{sender_id}]: '\{req_payload}'")

    // 5. Broker addresses reply directly back to sender_id
    println("[Broker] Routing response back to [\{sender_id}]...")
    let broker_rep : Array[Bytes] = [
      broker_msg.frames[0],
      b"",
      b"{\"id\": 1001, \"name\": \"MoonBit Hacker\", \"status\": \"active\"}",
    ]
    let _ = router.send(@moon_zmq.Message::multipart(broker_rep)) catch {
      err => {
        println("Broker reply error: \{err}")
        return
      }
    }

    // 6. Client receives reply
    let client_rep = client.recv() catch {
      err => {
        println("Client recv error: \{err}")
        return
      }
    }
    let rep_payload = @moon_zmq.ascii_bytes_to_string(
      client_rep.frames[client_rep.frames.length() - 1],
    )
    println("[Client-Alpha] Received RPC response: '\{rep_payload}'")

    client.close()
    router.close()
    ctx.term()
    println("=== ROUTER/DEALER Broker Completed Successfully ===")
  })
}
