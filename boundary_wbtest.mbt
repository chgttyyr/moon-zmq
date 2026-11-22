///|
/// Comprehensive boundary, fault-injection, and extreme robustness test suite.

///|
async test "boundary: malformed greeting header rejection" {
  @async.with_task_group(async fn(group) {
    let server = Socket::new(Dealer)
    let ep = server.bind(group, "tcp://127.0.0.1:0")

    let addr = parse_tcp_endpoint(ep)
    let conn = @socket.Tcp::connect(addr)

    // Send malformed greeting (invalid signature)
    let bad_greeting : Array[Byte] = Array(capacity=64)
    bad_greeting.push(b'\xFF')
    for _ in 1..<10 {
      bad_greeting.push(b'\x00')
    }
    for _ in 10..<64 {
      bad_greeting.push(b'\x00')
    }
    conn.write(Bytes::from_array(bad_greeting))

    // Server should reject and close the socket
    @async.sleep(50)
    // First read may get server's own greeting sent before receiving ours
    let _ = conn.read()
    let chunk2 = conn.read()
    let is_closed = match chunk2 {
      None => true
      Some(b) => b.length() == 0
    }
    assert_true(is_closed)

    conn.close()
    server.close()
  })
}

///|
async test "boundary: frame decoder reserved flag bit violation" {
  let decoder = FrameDecoder::new()
  // Feed a frame with reserved flag bits set (0xF8)
  let bad_frame : Bytes = Bytes::from_array([
    b'\xF8', b'\x05', b'a', b'b', b'c', b'd', b'e',
  ])
  let mut got_protocol_error = false
  let _ = decoder.feed(bad_frame) catch {
    ProtocolError(_) => {
      got_protocol_error = true
      []
    }
    _ => []
  }
  assert_true(got_protocol_error)
}

///|
async test "boundary: command frame with forbidden MORE flag (RFC 23 violation)" {
  let decoder = FrameDecoder::new()
  // 0x04 (COMMAND) | 0x01 (MORE) = 0x05
  let bad_frame : Bytes = Bytes::from_array([
    b'\x05', b'\x04', b'\x04', b'P', b'I', b'N', b'G',
  ])
  let mut got_error = false
  let _ = decoder.feed(bad_frame) catch {
    ProtocolError(_) => {
      got_error = true
      []
    }
    _ => []
  }
  assert_true(got_error)
}

///|
async test "boundary: maximum frame size enforcement" {
  @async.with_task_group(async fn(group) {
    let opts = SocketOptions::new()
    opts.set_max_frame_size(100) // Small limit: 100 bytes

    let server = Socket::new(Pull, options=opts)
    let ep = server.bind(group, "tcp://127.0.0.1:0")

    let client = Socket::new(Push)
    client.connect(group, ep)

    while client.active_peer_count() < 1 || server.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Small frame (50 bytes) -> succeeds
    let small_payload = make_benchmark_payload(50)
    client.send(Message::from_bytes(small_payload))
    let r1 = server.recv()
    assert_eq(r1.first_bytes().length(), 50)

    // Oversized frame (500 bytes > 100 bytes limit) -> peer drops connection
    let big_payload = make_benchmark_payload(500)
    client.send(Message::from_bytes(big_payload))

    @async.sleep(50)
    // Server drops pipe due to FrameTooLarge
    assert_eq(server.active_peer_count(), 0)

    client.close()
    server.close()
  })
}

///|
async test "boundary: rapid connection churn (40 connects and disconnects)" {
  @async.with_task_group(async fn(group) {
    let server = Socket::new(Router)
    let ep = server.bind(group, "tcp://127.0.0.1:0")

    for i in 0..<40 {
      let client = Socket::new(Dealer)
      client.connect(group, ep)

      while client.active_peer_count() < 1 {
        @async.sleep(5)
      }

      client.send_string("churn_\{i}")
      let s_msg = server.recv()
      assert_eq(ascii_bytes_to_string(s_msg.frames[1]), "churn_\{i}")

      // Rapidly close client
      client.close()
      @async.sleep(5)
    }

    // Verify server remains healthy and can still accept new connections
    let final_client = Socket::new(Dealer)
    final_client.connect(group, ep)
    while final_client.active_peer_count() < 1 {
      @async.sleep(5)
    }
    final_client.send_string("final_alive")
    let final_msg = server.recv()
    assert_eq(ascii_bytes_to_string(final_msg.frames[1]), "final_alive")

    final_client.close()
    server.close()
  })
}

///|
async test "boundary: operations on closed socket raise Closed error" {
  let sock = Socket::new(Pair)
  sock.close()

  assert_true(sock.is_closed())
  assert_false(sock.can_recv())
  assert_false(sock.can_send())

  let mut send_raised_closed = false
  let _ = sock.send_string("test") catch {
    Closed => {
      send_raised_closed = true
      ()
    }
    _ => ()
  }
  assert_true(send_raised_closed)

  let mut recv_raised_closed = false
  let _ = sock.recv() catch {
    Closed => {
      recv_raised_closed = true
      Message::from_string("")
    }
    _ => Message::from_string("")
  }
  assert_true(recv_raised_closed)
}
