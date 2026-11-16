///|
/// Unit & whitebox tests for RFC 24 PLAIN security mechanism.

test "plain: hello encoding and decoding" {
  let u = "admin"
  let p = "secret123"
  let frame_bytes = encode_plain_hello(u, p)
  let body = frame_bytes[2:].to_owned()
  let (decoded_u, decoded_p) = decode_plain_hello(body)
  assert_eq(decoded_u, u)
  assert_eq(decoded_p, p)
}

test "plain: welcome encoding and check" {
  let welcome_frame = encode_plain_welcome()
  let body = welcome_frame[2:].to_owned()
  assert_true(is_plain_welcome(body))
}

test "plain: initiate encoding and decoding" {
  let meta = Metadata::new("REQ", identity=Some(b"client-1"))
  let init_frame = encode_plain_initiate(meta)
  let body = init_frame[2:].to_owned()
  let decoded_meta = decode_plain_initiate(body)
  assert_eq(decoded_meta.socket_type, "REQ")
  assert_eq(decoded_meta.identity, Some(b"client-1"))
}

test "plain: malformed hello rejected" {
  let bad_body = b"\x05HEL"
  let mut is_err = false
  try {
    let _ = decode_plain_hello(bad_body)
    ()
  } catch {
    _ => is_err = true
  }
  assert_true(is_err)
}

async test "plain: async authentication success with valid credentials" {
  @async.with_task_group(async fn(group) {
    let ctx = Context::new()
    let server = ctx.socket(Rep)
    server.set_plain_server(
      true,
      authenticator=Some(fn(u, p) { u == "alice" && p == "wonderland" }),
    )

    let ep = server.bind(group, "tcp://127.0.0.1:0")

    let client = ctx.socket(Req)
    client.set_plain_credentials("alice", "wonderland")
    client.connect(group, ep)

    while server.active_peer_count() < 1 || client.active_peer_count() < 1 {
      @async.sleep(10)
    }

    client.send(Message::from_string("hello secure zmq"))
    let req = server.recv()
    assert_eq(req.first_string(), "hello secure zmq")

    server.send(Message::from_string("auth confirmed"))
    let rep = client.recv()
    assert_eq(rep.first_string(), "auth confirmed")

    client.close()
    server.close()
  })
}

async test "plain: async authentication rejection with invalid credentials" {
  @async.with_task_group(async fn(group) {
    let ctx = Context::new()
    let server = ctx.socket(Rep)
    server.set_plain_server(
      true,
      authenticator=Some(fn(u, p) { u == "alice" && p == "correct-pass" }),
    )

    let ep = server.bind(group, "tcp://127.0.0.1:0")

    let client = ctx.socket(Req)
    client.set_plain_credentials("alice", "wrong-pass")
    client.connect(group, ep)

    // Wait a brief period for handshake attempt to fail
    @async.sleep(50)

    // Client should have 0 active peers because authentication was rejected
    assert_eq(client.active_peer_count(), 0)

    client.close()
    server.close()
  })
}

