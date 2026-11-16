///|
test "REQ and REP envelope wrap/split/attach roundtrip" {
  let req_payload = Message::multipart([b"hello", b"world"])
  let wrapped = wrap_req_envelope(req_payload)
  assert_eq(wrapped.frames.length(), 3)
  assert_eq(wrapped.frames[0].length(), 0)

  let unwrapped = unwrap_req_envelope(wrapped)
  assert_true(unwrapped is Some(_))
  let req_unwrapped = unwrapped.unwrap()
  assert_eq(req_unwrapped.frames.length(), 2)
  assert_eq(req_unwrapped.first_string(), "hello")

  // REP envelope with simulated multi-hop ROUTER ids: [router_id, empty_delim, payload]
  let rep_incoming = Message::multipart([b"router-1", b"", b"ping"])
  let split_res = split_rep_envelope(rep_incoming)
  assert_true(split_res is Some(_))
  let (envelope, payload) = split_res.unwrap()
  assert_eq(envelope.length(), 2)
  assert_eq(envelope[0], b"router-1")
  assert_eq(envelope[1], b"")
  assert_eq(payload.first_string(), "ping")

  let reply_attached = attach_rep_envelope(
    envelope,
    Message::from_string("pong"),
  )
  assert_eq(reply_attached.frames.length(), 3)
  assert_eq(reply_attached.frames[0], b"router-1")
  assert_eq(reply_attached.frames[1], b"")
  assert_eq(ascii_bytes_to_string(reply_attached.frames[2]), "pong")
}

///|
async test "REQ and REP end-to-end exchange with alternation enforcement" {
  @async.with_task_group(group => {
    let rep = Socket::new(Rep)
    let ep = rep.bind(group, "tcp://127.0.0.1:0")

    let req = Socket::new(Req)
    req.connect(group, ep)

    // Cannot send before receiving on REP
    let mut rep_invalid = false
    let _ = rep.send_string("invalid-send") catch {
      InvalidState(_) => {
        rep_invalid = true
        ()
      }
      _ => ()
    }
    assert_true(rep_invalid)

    // Valid REQ -> REP exchange
    req.send_string("question-1")

    // REQ cannot send twice before receiving reply
    let mut req_double_send = false
    let _ = req.send_string("question-2") catch {
      InvalidState(_) => {
        req_double_send = true
        ()
      }
      _ => ()
    }
    assert_true(req_double_send)

    let req_msg = rep.recv_string()
    assert_eq(req_msg, "question-1")

    // REP replies
    rep.send_string("answer-1")
    let rep_msg = req.recv_string()
    assert_eq(rep_msg, "answer-1")

    // Second round of request/reply
    req.send_string("question-2")
    let req_msg2 = rep.recv_string()
    assert_eq(req_msg2, "question-2")
    rep.send_string("answer-2")
    let rep_msg2 = req.recv_string()
    assert_eq(rep_msg2, "answer-2")

    req.close()
    rep.close()
  })
}

///|
async test "PAIR socket bidirectional direct exchange" {
  @async.with_task_group(group => {
    let s1 = Socket::new(Pair)
    let ep = s1.bind(group, "tcp://127.0.0.1:0")

    let s2 = Socket::new(Pair)
    s2.connect(group, ep)

    s1.send_string("ping-from-s1")
    let r2 = s2.recv_string()
    assert_eq(r2, "ping-from-s1")

    s2.send_string("pong-from-s2")
    let r1 = s1.recv_string()
    assert_eq(r1, "pong-from-s2")

    s1.close()
    s2.close()
  })
}

///|
async test "PUSH and PULL round-robin load distribution" {
  @async.with_task_group(group => {
    let pull1 = Socket::new(Pull)
    let ep = pull1.bind(group, "tcp://127.0.0.1:0")

    let push = Socket::new(Push)
    push.connect(group, ep)

    // Direction constraints
    let mut push_cant_recv = false
    let _ = push.recv() catch {
      InvalidState(_) => {
        push_cant_recv = true
        Message::from_string("")
      }
      _ => Message::from_string("")
    }
    assert_true(push_cant_recv)

    let mut pull_cant_send = false
    let _ = pull1.send_string("bad") catch {
      InvalidState(_) => {
        pull_cant_send = true
        ()
      }
      _ => ()
    }
    assert_true(pull_cant_send)

    // Wait for connection to establish
    while push.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Send tasks across pipeline
    push.send_string("task-1")
    push.send_string("task-2")

    let t1 = pull1.recv_string()
    let t2 = pull1.recv_string()

    assert_eq(t1, "task-1")
    assert_eq(t2, "task-2")

    push.close()
    pull1.close()
  })
}

///|
async test "ROUTER and DEALER identity routing and router_mandatory" {
  @async.with_task_group(group => {
    let router_opts = SocketOptions::new()
    router_opts.set_router_mandatory(true)
    let router = Socket::new(Router, options=router_opts)
    let ep = router.bind(group, "tcp://127.0.0.1:0")

    let d1_opts = SocketOptions::new()
    d1_opts.set_identity(Some(b"worker-A"))
    let dealer1 = Socket::new(Dealer, options=d1_opts)

    let d2_opts = SocketOptions::new()
    d2_opts.set_identity(Some(b"worker-B"))
    let dealer2 = Socket::new(Dealer, options=d2_opts)

    dealer1.connect(group, ep)
    dealer2.connect(group, ep)

    dealer1.send_string("from-A")
    dealer2.send_string("from-B")

    // ROUTER receives: first frame is identity
    let m1 = router.recv()
    let m2 = router.recv()

    let id1 = m1.frames[0]
    let id2 = m2.frames[0]
    let body1 = ascii_bytes_to_string(m1.frames[1])
    let body2 = ascii_bytes_to_string(m2.frames[1])

    assert_true(id1 == b"worker-A" || id1 == b"worker-B")
    assert_true(id2 == b"worker-A" || id2 == b"worker-B")
    assert_true(body1 == "from-A" || body1 == "from-B")
    assert_true(body2 == "from-A" || body2 == "from-B")

    // Route targeted replies back to worker-A and worker-B
    router.send(Message::multipart([b"worker-A", b"reply-to-A"]))
    router.send(Message::multipart([b"worker-B", b"reply-to-B"]))

    let r_a = dealer1.recv_string()
    let r_b = dealer2.recv_string()
    assert_eq(r_a, "reply-to-A")
    assert_eq(r_b, "reply-to-B")

    // router_mandatory raises Unroutable when target does not exist
    let mut unroutable_err = false
    let _ = router.send(
      Message::multipart([b"ghost-worker", b"cannot-deliver"]),
    ) catch {
      Unroutable(_) => {
        unroutable_err = true
        ()
      }
      _ => ()
    }
    assert_true(unroutable_err)

    router.close()
    dealer1.close()
    dealer2.close()
  })
}

///|
async test "PUB and SUB topic prefix filtering" {
  @async.with_task_group(group => {
    let pub_sock = Socket::new(Pub)
    let ep = pub_sock.bind(group, "tcp://127.0.0.1:0")

    let sub_news = Socket::new(Sub)
    let sub_weather = Socket::new(Sub)

    sub_news.connect(group, ep)
    sub_weather.connect(group, ep)

    sub_news.subscribe_string("news.")
    sub_weather.subscribe_string("weather.")

    // Allow subscription propagation
    @async.sleep(50)

    // Publish on news, weather, and finance
    pub_sock.send_string("news.tech: moonbit 1.0 released")
    pub_sock.send_string("weather.tokyo: sunny 22C")
    pub_sock.send_string("finance.stock: market up 1%")
    pub_sock.send_string("news.world: global peace treaty")

    let n1 = sub_news.recv_string()
    let w1 = sub_weather.recv_string()
    let n2 = sub_news.recv_string()

    assert_eq(n1, "news.tech: moonbit 1.0 released")
    assert_eq(w1, "weather.tokyo: sunny 22C")
    assert_eq(n2, "news.world: global peace treaty")

    // Test unsubscribe
    sub_news.unsubscribe_string("news.")
    @async.sleep(30)
    pub_sock.send_string("news.tech: another article")

    pub_sock.close()
    sub_news.close()
    sub_weather.close()
  })
}

///|
async test "XPUB and XSUB subscription forwarding and event notifications" {
  @async.with_task_group(group => {
    let xpub_opts = SocketOptions::new()
    xpub_opts.set_xpub_verbose(true)
    let xpub = Socket::new(XPub, options=xpub_opts)
    let ep = xpub.bind(group, "tcp://127.0.0.1:0")

    let xsub = Socket::new(XSub)
    xsub.connect(group, ep)

    while xpub.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // XSUB sends a subscription frame
    xsub.send(
      Message::from_bytes(
        Bytes::from_array([b'\x01', b't', b'o', b'p', b'i', b'c']),
      ),
    )

    // Allow background reader on XPUB to read and post sub event
    @async.sleep(50)

    // XPUB receives the subscription event
    let sub_event = xpub.recv()
    assert_eq(sub_event.first_bytes()[0], b'\x01')
    assert_eq(
      ascii_bytes_to_string(sub_event.first_bytes()[1:].to_owned()),
      "topic",
    )

    // XPUB publishes data matching subscription
    xpub.send_string("topic message 1")
    let received = xsub.recv_string()
    assert_eq(received, "topic message 1")

    xpub.close()
    xsub.close()
  })
}
