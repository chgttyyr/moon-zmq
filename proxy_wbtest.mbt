///|
/// Whitebox tests for RFC 43 ZeroMQ Proxy and Steerable Proxy.
async test "proxy: bidirectional forwarding and message capture" {
  @async.with_task_group(async fn(group) {
    // 1. Setup Proxy frontend and backend (ROUTER <-> DEALER broker pattern)
    let frontend = Socket::new(Router)
    let frontend_ep = frontend.bind(group, "tcp://127.0.0.1:0")

    let backend = Socket::new(Dealer)
    let backend_ep = backend.bind(group, "tcp://127.0.0.1:0")

    // 2. Setup Capture socket (PULL sink)
    let capture_sink = Socket::new(Pull)
    let capture_ep = capture_sink.bind(group, "tcp://127.0.0.1:0")

    let capture_push = Socket::new(Push)
    capture_push.connect(group, capture_ep)

    // 3. Client connects to frontend (REQ)
    let client = Socket::new(Req)
    client.connect(group, frontend_ep)

    // 4. Worker connects to backend (REP)
    let worker = Socket::new(Rep)
    worker.connect(group, backend_ep)

    // 5. Start Proxy in background
    let proxy_task = group.spawn(no_wait=true, allow_failure=true, () => {
      let _ = proxy(frontend, backend, capture=Some(capture_push)) catch {
        _ => ()
      }
    })

    while frontend.active_peer_count() < 1 ||
          backend.active_peer_count() < 1 ||
          client.active_peer_count() < 1 ||
          worker.active_peer_count() < 1 ||
          capture_push.active_peer_count() < 1 ||
          capture_sink.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Client sends request
    client.send_string("request_data_123")

    // Worker receives forwarded request
    let worker_req = worker.recv()
    assert_eq(worker_req.first_string(), "request_data_123")

    // Worker replies
    worker.send_string("response_data_456")

    // Client receives forwarded reply
    let client_rep = client.recv()
    assert_eq(client_rep.first_string(), "response_data_456")

    // Verify capture received copies
    let cap_req = capture_sink.recv()
    let last_req_frame = cap_req.frames[cap_req.frames.length() - 1]
    assert_eq(ascii_bytes_to_string(last_req_frame), "request_data_123")

    let cap_rep = capture_sink.recv()
    let last_rep_frame = cap_rep.frames[cap_rep.frames.length() - 1]
    assert_eq(ascii_bytes_to_string(last_rep_frame), "response_data_456")

    proxy_task.cancel()
    client.close()
    worker.close()
    frontend.close()
    backend.close()
    capture_push.close()
    capture_sink.close()
  })
}

///|
async test "proxy: steerable control with PAUSE, RESUME and TERMINATE" {
  @async.with_task_group(async fn(group) {
    let frontend = Socket::new(Pair)
    let f_ep = frontend.bind(group, "tcp://127.0.0.1:0")

    let backend = Socket::new(Pair)
    let b_ep = backend.bind(group, "tcp://127.0.0.1:0")

    let ctrl_srv = Socket::new(Pair)
    let c_ep = ctrl_srv.bind(group, "tcp://127.0.0.1:0")

    let cli = Socket::new(Pair)
    cli.connect(group, f_ep)

    let wrk = Socket::new(Pair)
    wrk.connect(group, b_ep)

    let ctrl_cli = Socket::new(Pair)
    ctrl_cli.connect(group, c_ep)

    let stats = ProxyStats::new()
    let proxy_task = group.spawn(no_wait=true, allow_failure=true, () => {
      let _ = proxy_steerable(frontend, backend, control=Some(ctrl_srv), stats~) catch {
        _ => ()
      }
    })

    while frontend.active_peer_count() < 1 ||
          backend.active_peer_count() < 1 ||
          ctrl_srv.active_peer_count() < 1 ||
          ctrl_cli.active_peer_count() < 1 ||
          cli.active_peer_count() < 1 ||
          wrk.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // 1. Initial active forward
    cli.send_string("msg_1")
    let r1 = wrk.recv_string()
    assert_eq(r1, "msg_1")

    // 2. PAUSE command
    ctrl_cli.send_string("PAUSE")
    @async.sleep(30)

    // Message sent while paused should NOT be forwarded immediately
    cli.send_string("msg_paused")
    @async.sleep(50)
    assert_false(wrk.can_recv())

    // 3. RESUME command
    ctrl_cli.send_string("RESUME")
    @async.sleep(30)

    // Now it should be forwarded
    let r2 = wrk.recv_string()
    assert_eq(r2, "msg_paused")

    // 4. TERMINATE command
    ctrl_cli.send_string("TERMINATE")
    @async.sleep(50)

    proxy_task.cancel()
    cli.close()
    wrk.close()
    ctrl_cli.close()
    ctrl_srv.close()
    frontend.close()
    backend.close()
  })
}
