///|
test "parse_tcp_endpoint and exponential backoff" {
  let addr1 = parse_tcp_endpoint("tcp://127.0.0.1:5555")
  assert_eq("\{addr1}", "127.0.0.1:5555")
  let addr2 = parse_tcp_endpoint("tcp://localhost:9000")
  assert_eq("\{addr2}", "127.0.0.1:9000")
  let addr3 = parse_tcp_endpoint("tcp://*:0", for_bind=true)
  assert_eq("\{addr3}", "0.0.0.0:0")

  // Invalid endpoints
  let mut bad_scheme = false
  let _ = parse_tcp_endpoint("ipc:///tmp/test") catch {
    InvalidEndpoint(_) => {
      bad_scheme = true
      addr1
    }
    _ => addr1
  }
  assert_true(bad_scheme)

  let mut bad_port_zero_connect = false
  let _ = parse_tcp_endpoint("tcp://127.0.0.1:0", for_bind=false) catch {
    InvalidEndpoint(_) => {
      bad_port_zero_connect = true
      addr1
    }
    _ => addr1
  }
  assert_true(bad_port_zero_connect)

  // Exponential backoff calculation
  let d0 = 50
  let d1 = next_reconnect_delay_ms(d0, 50, 500)
  let d2 = next_reconnect_delay_ms(d1, 50, 500)
  let d3 = next_reconnect_delay_ms(d2, 50, 500)
  let d4 = next_reconnect_delay_ms(d3, 50, 500)
  assert_eq(d1, 100)
  assert_eq(d2, 200)
  assert_eq(d3, 400)
  assert_eq(d4, 500)
}

///|
async test "pipe high-water mark (HWM) drop, error, and block policies" {
  @async.with_task_group(group => {
    let meta = Metadata::new("DEALER", identity=Some(b"peer-1"))
    let pipe = Pipe::new(1, meta, sndhwm=2, rcvhwm=2)
    let m1 = Message::from_string("m1")
    let m2 = Message::from_string("m2")
    let m3 = Message::from_string("m3")

    // Fill outbound up to sndhwm=2
    assert_true(pipe.send_message(m1, policy=ErrorOnFull))
    assert_true(pipe.send_message(m2, policy=ErrorOnFull))
    assert_eq(pipe.snd_count, 2)
    assert_false(pipe.can_send())

    // DropOnFull returns false without raising
    let sent_drop = pipe.send_message(m3, policy=DropOnFull)
    assert_false(sent_drop)
    assert_eq(pipe.snd_count, 2)

    // ErrorOnFull raises HighWaterMarkReached
    let mut hwm_err = false
    let _ = pipe.send_message(m3, policy=ErrorOnFull) catch {
      HighWaterMarkReached => {
        hwm_err = true
        false
      }
      _ => false
    }
    assert_true(hwm_err)

    // BlockOnFull waits until consumer dequeues a frame
    let consumer = group.spawn(() => {
      @async.sleep(20)
      let w1 = pipe.dequeue_outbound()
      assert_true(w1 is Some(_))
    })
    let sent_block = pipe.send_message(m3, policy=BlockOnFull)
    assert_true(sent_block)
    consumer.wait()

    // Inbound rcvhwm=2 with drop_on_full=true drops excess inbound messages
    assert_true(pipe.push_inbound(m1, drop_on_full=true))
    assert_true(pipe.push_inbound(m2, drop_on_full=true))
    assert_false(pipe.push_inbound(m3, drop_on_full=true))
    assert_eq(pipe.rcv_count, 2)
    let r1 = pipe.recv_message()
    assert_eq(r1.first_string(), "m1")
    pipe.close()
  })
}

///|
async test "transport TCP bind/connect and 1-to-N multi-connection exchange" {
  @async.with_task_group(group => {
    let server_hub = TransportHub::new(Router)
    let endpoint = server_hub.bind(group, "tcp://127.0.0.1:0")

    let c1_opts = SocketOptions::new()
    c1_opts.set_identity(Some(b"client-1"))
    let c2_opts = SocketOptions::new()
    c2_opts.set_identity(Some(b"client-2"))
    let c3_opts = SocketOptions::new()
    c3_opts.set_identity(Some(b"client-3"))

    let c1 = TransportHub::new(Dealer, options=c1_opts)
    let c2 = TransportHub::new(Dealer, options=c2_opts)
    let c3 = TransportHub::new(Dealer, options=c3_opts)

    c1.connect(group, endpoint)
    c2.connect(group, endpoint)
    c3.connect(group, endpoint)

    c1.send_round_robin(Message::from_string("hello-from-1"))
    c2.send_round_robin(Message::from_string("hello-from-2"))
    c3.send_round_robin(Message::from_string("hello-from-3"))

    let seen : Array[String] = []
    for i = 0; i < 3; i = i + 1 {
      let (pipe, msg) = server_hub.recv_fair_queue()
      seen.push(msg.first_string())
      let _ = pipe.send_message(
        Message::from_string("ack:\{msg.first_string()}"),
      )
    }
    assert_eq(server_hub.active_peer_count(), 3)
    assert_true(seen.contains("hello-from-1"))
    assert_true(seen.contains("hello-from-2"))
    assert_true(seen.contains("hello-from-3"))

    let (_, r1) = c1.recv_fair_queue()
    let (_, r2) = c2.recv_fair_queue()
    let (_, r3) = c3.recv_fair_queue()
    assert_eq(r1.first_string(), "ack:hello-from-1")
    assert_eq(r2.first_string(), "ack:hello-from-2")
    assert_eq(r3.first_string(), "ack:hello-from-3")

    c1.close()
    c2.close()
    c3.close()
    server_hub.close()
  })
}

///|
async test "transport automatic reconnect after server disconnect and restart" {
  @async.with_task_group(group => {
    let server1 = TransportHub::new(Dealer)
    let endpoint = server1.bind(group, "tcp://127.0.0.1:0")

    let client_opts = SocketOptions::new()
    client_opts.set_reconnect_interval(20, max_ms=100)
    let client = TransportHub::new(Dealer, options=client_opts)
    client.connect(group, endpoint)

    // First message over initial connection
    client.send_round_robin(Message::from_string("before-disconnect"))
    let (_, msg1) = server1.recv_fair_queue()
    assert_eq(msg1.first_string(), "before-disconnect")

    // Shut down server1 to trigger disconnect on client
    server1.close()
    @async.sleep(50)
    assert_true(client.reconnect_count() >= 1)

    // Restart server2 on the same endpoint; client automatically reconnects
    let server2 = TransportHub::new(Dealer)
    let _ = server2.bind(group, endpoint)

    client.send_round_robin(Message::from_string("after-reconnect"))
    let (_, msg2) = server2.recv_fair_queue()
    assert_eq(msg2.first_string(), "after-reconnect")

    client.close()
    server2.close()
  })
}
