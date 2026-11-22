///|
/// Whitebox tests for SocketEvent monitoring, metrics tracking, batch operations,
/// and extended Message ergonomics.

///|
async test "monitor: socket lifecycle events and metrics tracking" {
  @async.with_task_group(async fn(group) {
    let server = Socket::new(Router)
    let srv_events = server.monitor_events()
    let ep = server.bind(group, "tcp://127.0.0.1:0")

    let client = Socket::new(Dealer)
    let _ = client.monitor_events()
    client.connect(group, ep)

    while client.active_peer_count() < 1 || server.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Client sends two messages to server
    client.send_string("event_msg_1")
    client.send_string("event_msg_2")

    let m1 = server.recv()
    let m2 = server.recv()
    assert_eq(ascii_bytes_to_string(m1.frames[1]), "event_msg_1")
    assert_eq(ascii_bytes_to_string(m2.frames[1]), "event_msg_2")

    // Verify metrics
    assert_eq(client.messages_sent(), 2L)
    assert_eq(server.messages_received(), 2L)
    assert_true(client.bytes_sent() > 0L)
    assert_true(server.bytes_received() > 0L)

    // Server replies
    let r1 = attach_rep_envelope([m1.frames[0]], Message::from_string("rep_1"))
    server.send(r1)
    let c_rep = client.recv()
    assert_eq(c_rep.first_string(), "rep_1")
    assert_eq(server.messages_sent(), 1L)
    assert_eq(client.messages_received(), 1L)

    // Verify active peer IDs
    let peers = server.active_peer_ids()
    assert_eq(peers.length(), 1)

    // Close client and check Disconnected event
    client.close()
    @async.sleep(50)

    server.close()

    // Inspect collected events
    let mut saw_listening = false
    let mut saw_handshake = false
    let mut saw_close = false
    while true {
      match srv_events.try_get() {
        Some(ev) =>
          match ev {
            Listening(_) => saw_listening = true
            HandshakeSucceeded(_, _) => saw_handshake = true
            Closed => saw_close = true
            _ => ()
          }
        None => break
      }
    }
    assert_true(saw_listening)
    assert_true(saw_handshake)
    assert_true(saw_close)
  })
}

///|
async test "batch: send_batch and recv_batch operations" {
  @async.with_task_group(async fn(group) {
    let pull = Socket::new(Pull)
    let ep = pull.bind(group, "tcp://127.0.0.1:0")

    let push = Socket::new(Push)
    push.connect(group, ep)

    while push.active_peer_count() < 1 || pull.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Prepare batch of 15 messages
    let batch_out : Array[Message] = []
    for i in 0..<15 {
      batch_out.push(Message::from_string("batch_item_\{i}"))
    }

    let sent_count = push.send_batch(batch_out)
    assert_eq(sent_count, 15)

    // Allow messages to arrive in inbound queue
    @async.sleep(50)

    let batch_in = pull.recv_batch(max_count=20)
    assert_eq(batch_in.length(), 15)
    for i = 0; i < 15; i = i + 1 {
      assert_eq(batch_in[i].first_string(), "batch_item_\{i}")
    }

    push.close()
    pull.close()
  })
}

///|
test "message: extended multipart ergonomics and inspection" {
  // 1. from_strings & to_string_array
  let strings = ["part_A", "part_B", "part_C"]
  let msg = Message::from_strings(strings)
  assert_eq(msg.frame_count(), 3)
  assert_false(msg.is_empty())
  assert_eq(msg.total_bytes(), 18) // 6 + 6 + 6

  let extracted = msg.to_string_array()
  assert_eq(extracted.length(), 3)
  assert_eq(extracted[0], "part_A")
  assert_eq(extracted[1], "part_B")
  assert_eq(extracted[2], "part_C")

  // 2. frame_at & frame_string_at
  assert_eq(msg.frame_string_at(0), Some("part_A"))
  assert_eq(msg.frame_string_at(2), Some("part_C"))
  assert_eq(msg.frame_string_at(3), None)
  assert_eq(msg.frame_string_at(-1), None)

  // 3. add_frame & add_string
  let extended = msg.add_string("part_D")
  assert_eq(extended.frame_count(), 4)
  assert_eq(extended.frame_string_at(3), Some("part_D"))
  // Original msg remains immutable
  assert_eq(msg.frame_count(), 3)
}
