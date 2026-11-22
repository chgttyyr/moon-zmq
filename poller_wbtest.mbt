///|
/// Whitebox tests for the ZeroMQ `Poller` multiplexer.
test "poller: add, modify, remove and length management" {
  let s1 = Socket::new(Pair)
  let s2 = Socket::new(Pair)
  let s3 = Socket::new(Pair)

  let poller = Poller::new()
  assert_eq(poller.length(), 0)

  poller.add(s1, POLLIN)
  poller.add(s2, POLLOUT)
  assert_eq(poller.length(), 2)

  // Re-adding modifies existing
  poller.add(s1, POLLIN | POLLOUT)
  assert_eq(poller.length(), 2)

  // Modify
  assert_true(poller.modify(s2, POLLIN))
  assert_false(poller.modify(s3, POLLIN))

  // Remove
  assert_true(poller.remove(s1))
  assert_false(poller.remove(s1))
  assert_eq(poller.length(), 1)

  poller.clear()
  assert_eq(poller.length(), 0)

  s1.close()
  s2.close()
  s3.close()
}

///|
async test "poller: timeout on empty channels" {
  @async.with_task_group(async fn(group) {
    let rep = Socket::new(Rep)
    let ep = rep.bind(group, "tcp://127.0.0.1:0")
    let req = Socket::new(Req)
    req.connect(group, ep)

    while rep.active_peer_count() < 1 || req.active_peer_count() < 1 {
      @async.sleep(10)
    }

    let poller = Poller::new()
    poller.add(rep, POLLIN)

    // Should time out with 0 ready items within ~30ms
    let count = poller.poll(timeout_ms=30)
    assert_eq(count, 0)
    assert_false(poller.has_in(rep))

    req.close()
    rep.close()
  })
}

///|
async test "poller: multiplexing multiple active sockets" {
  @async.with_task_group(async fn(group) {
    // Pair channel 1
    let p1_srv = Socket::new(Pair)
    let ep1 = p1_srv.bind(group, "tcp://127.0.0.1:0")
    let p1_cli = Socket::new(Pair)
    p1_cli.connect(group, ep1)

    // Pair channel 2
    let p2_srv = Socket::new(Pair)
    let ep2 = p2_srv.bind(group, "tcp://127.0.0.1:0")
    let p2_cli = Socket::new(Pair)
    p2_cli.connect(group, ep2)

    while p1_srv.active_peer_count() < 1 || p2_srv.active_peer_count() < 1 {
      @async.sleep(10)
    }

    let poller = Poller::new()
    poller.add(p1_srv, POLLIN)
    poller.add(p2_srv, POLLIN)

    // 1. Send only on channel 2
    p2_cli.send_string("event_on_ch2")
    @async.sleep(20)

    let count1 = poller.poll(timeout_ms=100)
    assert_true(count1 >= 1)
    assert_false(poller.has_in(p1_srv))
    assert_true(poller.has_in(p2_srv))

    let msg2 = p2_srv.recv_string()
    assert_eq(msg2, "event_on_ch2")

    // 2. Send on channel 1
    p1_cli.send_string("event_on_ch1")
    @async.sleep(20)

    let count2 = poller.poll(timeout_ms=100)
    assert_true(count2 >= 1)
    assert_true(poller.has_in(p1_srv))
    assert_false(poller.has_in(p2_srv))

    let msg1 = p1_srv.recv_string()
    assert_eq(msg1, "event_on_ch1")

    p1_cli.close()
    p1_srv.close()
    p2_cli.close()
    p2_srv.close()
  })
}
