///|
/// Comprehensive whitebox test suite covering RFC 32 Z85, binary wire cursors,
/// `inproc://` zero-copy transport (including connect-before-bind), prefix
/// `SubscriptionTrie`, `LastValueCache` + LVC proxy, `ConflateBuffer`, RFC 27 ZAP 1.0,
/// RFC 7 Majordomo (`MDP/0.2`) + RFC 8 `MMI`, `ReliableClient` failover, `Radio`/`Dish`
/// group pub-sub, RFC 4 `ZConfig` ZPL, `ZActor`, `ZLoop`, and `diagnostics`.

///|
test "z85: RFC 32 canonical HelloWorld vector, 32-byte key, and padded roundtrip" {
  // Official ZeroMQ RFC 32 test vector: 8 bytes -> "HelloWorld"
  let hello_bin = Bytes::from_array([
    b'\x86', b'\x4F', b'\xD2', b'\x6F', b'\xB5', b'\x59', b'\xF7', b'\x5B',
  ])
  let encoded = z85_encode(hello_bin)
  assert_eq(encoded, "HelloWorld")
  let decoded = z85_decode("HelloWorld")
  assert_eq(decoded, hello_bin)
  assert_true(is_valid_z85("HelloWorld"))
  assert_false(is_valid_z85("HelloWorld!")) // length 11 not divisible by 5
  assert_false(is_valid_z85("#####")) // 84*85^4+... > 0xFFFFFFFF overflow

  // Official RFC 32 CURVE 32-byte key vector
  let curve_z85 = "JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6"
  let curve_raw = z85_decode(curve_z85)
  assert_eq(curve_raw.length(), 32)
  assert_eq(z85_encode(curve_raw), curve_z85)

  // Padded Z85 roundtrip for arbitrary non-multiple-of-4 lengths
  for len = 0; len <= 15; len = len + 1 {
    let buf : Array[Byte] = Array(capacity=len)
    for i = 0; i < len; i = i + 1 {
      buf.push(((i * 37 + 11) & 0xFF).to_byte())
    }
    let orig = Bytes::from_array(buf)
    let p_enc = z85_encode_padded(orig)
    let p_dec = z85_decode_padded(p_enc)
    assert_eq(p_dec, orig)
  }

  // Hex roundtrip
  let hex = bytes_to_hex(hello_bin, uppercase=true)
  assert_eq(hex, "864FD26FB559F75B")
  assert_eq(hex_to_bytes("86:4f:d2:6f-b5 59 f7 5b"), hello_bin)
}

///|
test "wire_codec: binary WireWriter/WireReader and multipart blob packing" {
  let w = WireWriter::new()
  w.write_u8(240)
  w.write_u16_be(0x1234)
  w.write_u16_le(0xABCD)
  w.write_i32_be(0x01020304)
  w.write_i32_le(0x05060708)
  w.write_i64_be(0x0102030405060708L)
  w.write_i64_le(0x1112131415161718L)
  w.write_short_string("zmtp-short")
  w.write_long_string("zmtp-long-payload-value")
  w.write_chunk(b"\xDE\xAD\xBE\xEF")
  let meta_map : Map[String, String] = Map([
    ("Role", "worker"),
    ("Region", "cn-south"),
  ])
  w.write_string_map(meta_map)

  let r = WireReader::new(w.to_bytes())
  assert_eq(r.read_u8(), 240)
  assert_eq(r.read_u16_be(), 0x1234)
  assert_eq(r.read_u16_le(), 0xABCD)
  assert_eq(r.read_i32_be(), 0x01020304)
  assert_eq(r.read_i32_le(), 0x05060708)
  assert_eq(r.read_i64_be(), 0x0102030405060708L)
  assert_eq(r.read_i64_le(), 0x1112131415161718L)
  assert_eq(r.read_short_string(), "zmtp-short")
  assert_eq(r.read_long_string(), "zmtp-long-payload-value")
  assert_eq(r.read_chunk(), b"\xDE\xAD\xBE\xEF")
  let decoded_map = r.read_string_map()
  assert_eq(decoded_map.get("Role"), Some("worker"))
  assert_eq(decoded_map.get("Region"), Some("cn-south"))
  assert_true(r.is_eof())

  let multi = Message::from_strings(["env-id", "", "payload-part-1", "part-2"])
  let blob = pack_multipart_blob(multi)
  let unpacked = unpack_multipart_blob(blob)
  assert_eq(unpacked, multi)
}

///|
test "trie: prefix subscription multi-trie refcounts and disconnect cleanup" {
  let trie = SubscriptionTrie::new()
  // First subscriber on "market.btc" triggers 0 -> 1 upstream transition
  assert_true(trie.add(b"market.btc", 10))
  // Duplicate subscribe from same pipe and second pipe do NOT trigger 0 -> 1
  assert_false(trie.add(b"market.btc", 10))
  assert_false(trie.add(b"market.btc", 20))
  assert_true(trie.add(b"market.", 30))
  assert_eq(trie.unique_topic_count(), 2)
  assert_eq(trie.subscriber_count(b"market.btc"), 2)

  let matched_btc = trie.match_pipes(b"market.btc.usdt 98000")
  assert_eq(matched_btc.length(), 3) // pipes 30, 10, 20
  let matched_eth = trie.match_pipes(b"market.eth.usdt 3200")
  assert_eq(matched_eth, [30])
  assert_false(trie.matches_any(b"sports.football"))

  // Pipe 10 unsubscribes once (still has count=1) -> not dropped to 0
  assert_false(trie.rm(b"market.btc", 10))
  assert_false(trie.rm(b"market.btc", 10)) // pipe 20 still subscribed
  assert_eq(trie.subscriber_count(b"market.btc"), 1)

  // Disconnect pipe 20 -> "market.btc" drops to 0!
  let emptied = trie.remove_pipe(20)
  assert_eq(emptied, [b"market.btc"])
  assert_eq(trie.unique_topic_count(), 1)
}

///|
test "lvc_and_conflate: LastValueCache eviction/TTL and ConflateBuffer coalescing" {
  let cache = LastValueCache::new(max_topics=2, ttl_ms=500L)
  cache.store(b"sensor.temp", Message::from_string("24.5C"), now_ms=100L)
  cache.store(b"sensor.hum", Message::from_string("60%"), now_ms=200L)
  // Overwrite existing topic updates value without increasing topic count
  cache.store(b"sensor.temp", Message::from_string("25.1C"), now_ms=300L)
  assert_eq(cache.size(), 2)
  match cache.get_exact(b"sensor.temp", now_ms=400L) {
    Some(m) => assert_eq(m.first_string(), "25.1C")
    None => fail("expected cached sensor.temp")
  }
  // Adding 3rd topic evicts oldest sequence ("sensor.hum" seq=2)
  cache.store(b"sensor.press", Message::from_string("1013hPa"), now_ms=450L)
  assert_eq(cache.get_exact(b"sensor.hum", now_ms=460L), None)
  let hits = cache.query_prefix(b"sensor.", now_ms=500L)
  assert_eq(hits.length(), 2)
  // TTL expiry after 500ms
  assert_eq(cache.purge_expired(1000L), 2)
  assert_eq(cache.size(), 0)

  // ConflateBuffer single-slot and per-topic modes
  let single = ConflateBuffer::new(by_topic=false)
  single.push(Message::from_string("v1"))
  single.push(Message::from_string("v2"))
  single.push(Message::from_string("v3"))
  assert_eq(single.overwritten_count(), 2L)
  match single.try_pop() {
    Some(m) => assert_eq(m.first_string(), "v3")
    None => fail("expected conflated v3")
  }
  assert_false(single.has_message())

  let keyed = ConflateBuffer::new(by_topic=true)
  keyed.push(Message::from_strings(["A", "1"]))
  keyed.push(Message::from_strings(["B", "10"]))
  keyed.push(Message::from_strings(["A", "2"]))
  assert_eq(keyed.overwritten_count(), 1L)
  match keyed.try_pop() {
    Some(m1) => assert_eq(m1.to_string_array(), ["A", "2"])
    None => fail("expected topic A")
  }
  match keyed.try_pop() {
    Some(m2) => assert_eq(m2.to_string_array(), ["B", "10"])
    None => fail("expected topic B")
  }
}

///|
async test "inproc: zero-copy REQ/REP, connect-before-bind, and LVC proxy replay" {
  @async.with_task_group(async fn(group) {
    let ctx = Context::new()
    defer ctx.term()

    // 1. Connect-before-bind over inproc://
    let client = ctx.socket(Req)
    client.set_reconnect_interval(10, max_ms=50)
    client.connect(group, "inproc://early-rpc")

    @async.sleep(25)
    let server = ctx.socket(Rep)
    let bound_ep = server.bind(group, "inproc://early-rpc")
    assert_eq(bound_ep, "inproc://early-rpc")

    let srv_task = group.spawn(allow_failure=true, async fn() {
      let req = server.recv_string()
      server.send_string("ack:\{req}")
    })
    client.send_string("ping-inproc")
    let rep = client.recv_string()
    assert_eq(rep, "ack:ping-inproc")
    srv_task.wait()

    // 2. Last-Value Caching (LVC) XPUB/XSUB proxy over inproc://
    let xsub = ctx.socket(XSub)
    let xpub = ctx.socket(XPub)
    let ctrl_srv = ctx.socket(Pull)
    let ctrl_cli = ctx.socket(Push)
    let _ = xsub.bind(group, "inproc://lvc-front")
    let _ = xpub.bind(group, "inproc://lvc-back")
    let _ = ctrl_srv.bind(group, "inproc://lvc-ctrl")
    ctrl_cli.connect(group, "inproc://lvc-ctrl")

    let lvc_cache = LastValueCache::new()
    let lvc_task = group.spawn(allow_failure=true, async fn() {
      run_lvc_proxy(xsub, xpub, lvc_cache, control=Some(ctrl_srv))
    })

    let pub_sock = ctx.socket(Pub)
    pub_sock.connect(group, "inproc://lvc-front")
    // Prime upstream subscription through an initial subscriber
    let seed_sub = ctx.socket(Sub)
    seed_sub.subscribe_string("ticker.")
    seed_sub.connect(group, "inproc://lvc-back")
    @async.sleep(80)

    pub_sock.send_string("ticker.mbt 188.50")
    let first_got = seed_sub.recv_string()
    assert_eq(first_got, "ticker.mbt 188.50")

    // Late-joining subscriber connects AFTER publication and immediately gets LVC replay!
    let late_sub = ctx.socket(Sub)
    late_sub.subscribe_string("ticker.mbt")
    late_sub.connect(group, "inproc://lvc-back")
    let replayed = late_sub.recv_string()
    assert_eq(replayed, "ticker.mbt 188.50")

    ctrl_cli.send_string("TERMINATE")
    let replay_total = lvc_task.wait()
    assert_true(replay_total >= 1L)
  })
}

///|
async test "zap: RFC 27 ZAP 1.0 server over inproc://zeromq.zap.01 with CIDR & PLAIN" {
  @async.with_task_group(async fn(group) {
    let ctx = Context::new()
    defer ctx.term()

    let engine = ZapPolicyEngine::new()
    engine.allow_ip("127.0.0.0/8")
    engine.allow_ip("10.20.0.0/16")
    engine.block_ip("10.20.99.0/24")
    engine.upsert_plain_user("alice", "s3cr3t!", domain="trading", properties=[
      ("Role", "admin"),
    ])

    let _ = engine.spawn_inproc_server(group, ctx)
    let zap_cli = ctx.socket(Req)
    zap_cli.connect(group, ZAP_DEFAULT_ENDPOINT)

    // 1. Valid PLAIN user from allowed CIDR
    let ok_req = ZapRequest::new_plain(
      b"req-1",
      "10.20.1.5",
      "alice",
      "s3cr3t!",
      domain="trading",
    )
    let ok_rep = zap_authenticate_over_socket(zap_cli, ok_req)
    assert_true(ok_rep.is_authenticated())
    assert_eq(ok_rep.status_code, Ok200)
    assert_eq(ok_rep.user_id, "alice")
    let props = decode_zap_metadata_blob(ok_rep.metadata)
    assert_eq(props.length(), 2)

    // 2. Blocked subnet rejected with 400 even with valid password
    let blocked_req = ZapRequest::new_plain(
      b"req-2",
      "10.20.99.8",
      "alice",
      "s3cr3t!",
      domain="trading",
    )
    let blocked_rep = zap_authenticate_over_socket(zap_cli, blocked_req)
    assert_false(blocked_rep.is_authenticated())
    assert_eq(blocked_rep.status_code, Rejected400)

    // 3. Wrong password rejected with 400
    let bad_pw_req = ZapRequest::new_plain(
      b"req-3",
      "127.0.0.1",
      "alice",
      "wrong",
      domain="trading",
    )
    let bad_pw_rep = zap_authenticate_over_socket(zap_cli, bad_pw_req)
    assert_eq(bad_pw_rep.status_code, Rejected400)
    assert_eq(engine.audit_log.length(), 3)
  })
}

///|
test "majordomo: RFC 7 MDP/0.2 broker LRU routing, MMI discovery, and worker expiry" {
  let broker = MdpBroker::new(heartbeat_liveness_ms=1000L)

  // Before any worker registers, RFC 8 mmi.service returns "404"
  let mmi_404 = broker.handle_router_message(
    Message::multipart([b"cli-1", b"", b"MDPC02", b"mmi.service", b"echo"]),
    now_ms=100L,
  )
  assert_eq(mmi_404.length(), 1)
  assert_eq(mmi_404[0].frames[4], b"404")

  // Client queues request BEFORE worker joins; broker holds it safely
  let queued_out = broker.handle_router_message(
    Message::multipart([b"cli-1", b"", b"MDPC02", b"echo", b"hello-mdp"]),
    now_ms=120L,
  )
  assert_eq(queued_out.length(), 0)

  // Worker registers READY for "echo" -> broker immediately dispatches queued request!
  let ready_out = broker.handle_router_message(
    Message::multipart([b"wrk-1", b"", b"MDPW02", b"\x01", b"echo"]),
    now_ms=150L,
  )
  assert_eq(ready_out.length(), 1)
  assert_eq(ready_out[0].frames[0], b"wrk-1")
  assert_eq(ready_out[0].frames[3], b"\x02") // REQUEST
  assert_eq(ready_out[0].frames[4], b"cli-1")
  assert_eq(ready_out[0].frames[6], b"hello-mdp")

  // Now mmi.service returns "200"
  let mmi_200 = broker.handle_router_message(
    Message::multipart([b"cli-2", b"", b"MDPC02", b"mmi.service", b"echo"]),
    now_ms=160L,
  )
  assert_eq(mmi_200[0].frames[4], b"200")

  // Worker sends REPLY -> broker routes MDPC02 reply back to cli-1
  let reply_out = broker.handle_router_message(
    Message::multipart([
      b"wrk-1", b"", b"MDPW02", b"\x03", b"cli-1", b"", b"world-mdp",
    ]),
    now_ms=200L,
  )
  assert_eq(reply_out.length(), 1)
  assert_eq(reply_out[0].frames[0], b"cli-1")
  assert_eq(reply_out[0].frames[4], b"world-mdp")

  // Advance clock past heartbeat liveness (200 + 1000 < 1500) -> worker purged
  let purged = broker.purge_expired_workers(1500L)
  assert_eq(purged.length(), 1)
  assert_eq(broker.worker_count(), 0)
}

///|
async test "reliable_client_and_radio_dish: failover RPC and exact-group pubsub" {
  @async.with_task_group(async fn(group) {
    let ctx = Context::new()
    defer ctx.term()

    // 1. ReliableClient failover from unbound/dead endpoint to live REP server
    let live_srv = ctx.socket(Rep)
    let _ = live_srv.bind(group, "inproc://backup-rep")
    let srv_job = group.spawn(allow_failure=true, async fn() {
      let q = live_srv.recv_string()
      live_srv.send_string("failover-ok:\{q}")
    })

    let rc = ReliableClient::new(
      ctx,
      ["inproc://unreachable-primary", "inproc://backup-rep"],
      timeout_ms=45,
      max_retries=3,
    )
    defer rc.close()
    let ans = rc.request(group, Message::from_string("job-42"))
    assert_eq(ans.first_string(), "failover-ok:job-42")
    assert_true(rc.failovers() >= 1L)
    srv_job.wait()

    // 2. Radio / Dish exact group matching ("orders" vs "orders.archive")
    let radio = RadioSocket::new(ctx)
    let dish = DishSocket::new(ctx)
    let _ = radio.bind(group, "inproc://radio-bus")
    dish.join("orders")
    dish.connect(group, "inproc://radio-bus")
    @async.sleep(30)

    // Message to "orders.archive" must NOT match exact group "orders"
    radio.send_group_string("orders.archive", "old-order-1")
    radio.send_group_string("orders", "live-order-99")

    let (grp, msg) = dish.recv_group()
    assert_eq(grp, "orders")
    assert_eq(msg.first_string(), "live-order-99")
    assert_true(dish.leave("orders"))
    assert_false(dish.is_joined("orders"))
  })
}

///|
async test "reactor_and_zconfig: RFC 4 ZPL config, ZActor pipe, and ZLoop timers" {
  let zpl_doc =
    #|# ZeroMQ RFC 4 Configuration Sample
    #|context
    #|    iothreads = 2
    #|socket
    #|    identity = "router-east-01"
    #|    sndhwm = 4096
    #|    rcvhwm = 2048
    #|    router_mandatory = true
    #|    heartbeat_ivl_ms = 500
    #|    plain
    #|        username = "svc_user"
    #|        password = "pw#123=ok"
  let cfg = ZConfig::parse(zpl_doc)
  assert_eq(cfg.get_int("context/iothreads"), 2)
  assert_eq(cfg.get("socket/identity"), "router-east-01")
  assert_eq(cfg.get("socket/plain/password"), "pw#123=ok")

  let opts = SocketOptions::new()
  apply_zconfig_to_socket_options(cfg, opts)
  assert_eq(opts.sndhwm, 4096)
  assert_eq(opts.rcvhwm, 2048)
  assert_true(opts.router_mandatory)
  assert_eq(opts.heartbeat_ivl_ms, 500)
  assert_eq(opts.plain_credentials, Some(("svc_user", "pw#123=ok")))

  let roundtrip_cfg = ZConfig::parse(socket_options_to_zconfig(opts).to_zpl())
  assert_eq(roundtrip_cfg.get_int("socket/sndhwm"), 4096)

  // ZActor + ZLoop integration
  @async.with_task_group(async fn(group) {
    let ctx = Context::new()
    defer ctx.term()

    let actor = ZActor::spawn(group, ctx, async fn(pipe) {
      while true {
        let cmd = pipe.recv_string() catch { _ => break }
        if cmd == ZACTOR_TERM_CMD {
          break
        }
        pipe.send_string("ECHO:\{cmd}") catch {
          _ => break
        }
      }
    })

    let zloop = ZLoop::new()
    let replies : Array[String] = []
    let ticks : Ref[Int32] = { val: 0, }

    zloop.add_reader(actor.socket(), async fn(sock) {
      let s = sock.recv_string() catch { _ => return false }
      replies.push(s)
      replies.length() < 2
    })

    let _ = zloop.add_timer(15, times=2, fn(_tid) {
      ticks.val = ticks.val + 1
      true
    })

    actor.send_string("alpha")
    actor.send_string("beta")
    zloop.run()
    actor.stop()

    assert_eq(replies, ["ECHO:alpha", "ECHO:beta"])
  })
}

///|
test "diagnostics: ZMTP wire dissector, LatencyHistogram, RateLimiter, and CircuitBreaker" {
  let greet = Greeting::new(mechanism="NULL", as_server=false).encode()
  let ready = ZmtpCommand::Ready(Metadata::new("DEALER", identity=Some(b"d1"))).encode_frame()
  let data = Message::from_strings(["hdr", "body"]).encode()
  let full_wire = concat_bytes([greet, ready, data])

  let packets = dissect_zmtp_wire(full_wire, expect_greeting=true)
  assert_eq(packets.length(), 4) // Greeting + READY + 2 multipart frames
  let trace = format_dissected_trace(packets)
  assert_true(trace.contains("GREETING v3.1"))
  assert_true(trace.contains("CMD READY"))
  let dump = format_wire_hexdump(data, max_bytes=64)
  assert_true(dump.contains("0000"))

  // LatencyHistogram percentiles
  let hist = LatencyHistogram::new(window_capacity=128)
  for i = 1; i <= 100; i = i + 1 {
    hist.record_us((i * 10).to_int64())
  }
  let summary = hist.summarize()
  assert_eq(summary.samples, 100L)
  assert_eq(summary.min_us, 10L)
  assert_eq(summary.max_us, 1000L)
  assert_eq(summary.p50_us, 500L)
  assert_eq(summary.p99_us, 990L)

  // RateLimiter burst and token refill
  let limiter = RateLimiter::new(
    msgs_per_sec=100L,
    burst_msgs=2L,
    initial_now_ms=0L,
  )
  assert_true(limiter.try_acquire(64, 0L))
  assert_true(limiter.try_acquire(64, 0L))
  assert_false(limiter.try_acquire(64, 0L))
  assert_eq(limiter.wait_time_ms(64, 0L), 10)
  assert_true(limiter.try_acquire(64, 10L))

  // CircuitBreaker state transitions: Closed -> Open -> HalfOpen -> Closed
  let cb = CircuitBreaker::new(
    failure_threshold=2,
    success_threshold=2,
    cooldown_ms=100L,
  )
  assert_eq(cb.current_state(now_ms=0L), CircuitClosed)
  cb.record_failure(10L)
  cb.record_failure(20L)
  assert_eq(cb.current_state(now_ms=20L), CircuitOpen)
  assert_false(cb.allow_request(50L))
  assert_true(cb.allow_request(125L)) // Cooldown elapsed -> HalfOpen
  assert_eq(cb.current_state(now_ms=125L), CircuitHalfOpen)
  cb.record_success()
  cb.record_success()
  assert_eq(cb.current_state(now_ms=130L), CircuitClosed)
}
