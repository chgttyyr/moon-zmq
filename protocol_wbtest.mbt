///|
test "short and long frame encode/decode roundtrip" {
  // 0-byte empty short frame
  let f0 = Frame::data_frame(b"", more=false)
  let e0 = f0.encode()
  assert_eq(e0.length(), 2)
  assert_eq(e0[0].to_int(), 0)
  assert_eq(e0[1].to_int(), 0)

  // 255-byte boundary short frame with MORE flag
  let b255 = Bytes::make(255, b'A')
  let f255 = Frame::data_frame(b255, more=true)
  let e255 = f255.encode()
  assert_eq(e255.length(), 2 + 255)
  assert_eq(e255[0].to_int(), FLAG_MORE)
  assert_eq(e255[1].to_int(), 255)

  // 256-byte boundary long frame (8-byte big-endian size)
  let b256 = Bytes::make(256, b'B')
  let f256 = Frame::data_frame(b256, more=false)
  let e256 = f256.encode()
  assert_eq(e256.length(), 9 + 256)
  assert_eq(e256[0].to_int(), FLAG_LONG)
  assert_eq(e256[7].to_int(), 1)
  assert_eq(e256[8].to_int(), 0)

  // 70,000-byte long frame (> 64 KiB)
  let b70k = Bytes::make(70000, b'Z')
  let f70k = Frame::data_frame(b70k, more=false)
  let e70k = f70k.encode()
  let dec = FrameDecoder::new()
  let items = dec.feed(concat_bytes([e0, e255, e256, e70k]))
  assert_eq(items.length(), 3)
  assert_eq(items[0], DataMessage(Message::from_bytes(b"")))
  assert_eq(items[1], DataMessage(Message::multipart([b255, b256])))
  assert_eq(items[2], DataMessage(Message::from_bytes(b70k)))
}

///|
test "command frame interleaved inside multipart message stream" {
  let f1 = Frame::data_frame(b"part-1", more=true).encode()
  let ping_cmd = ZmtpCommand::Ping(100, b"ctx123").encode_frame()
  let f2 = Frame::data_frame(b"part-2", more=false).encode()
  let dec = FrameDecoder::new()
  let items = dec.feed(concat_bytes([f1, ping_cmd, f2]))
  assert_eq(items.length(), 2)
  match items[0] {
    CommandFrame(raw) => {
      let cmd = ZmtpCommand::decode_body(raw)
      assert_eq(cmd, Ping(100, b"ctx123"))
    }
    _ => assert_true(false)
  }
  assert_eq(items[1], DataMessage(Message::multipart([b"part-1", b"part-2"])))
}

///|
test "invalid frame inputs: reserved flags, command+more, oversized and negative length" {
  // 1. Non-zero reserved flag bit (0x08)
  let dec1 = FrameDecoder::new()
  let mut caught1 = false
  let _ = dec1.feed(b"\x08\x00") catch {
    ProtocolError(_) => {
      caught1 = true
      []
    }
    _ => []
  }
  assert_true(caught1)

  // 2. COMMAND frame with MORE flag set (0x04 | 0x01 = 0x05) is forbidden by RFC 23
  let dec2 = FrameDecoder::new()
  let mut caught2 = false
  let _ = dec2.feed(b"\x05\x04PING") catch {
    ProtocolError(_) => {
      caught2 = true
      []
    }
    _ => []
  }
  assert_true(caught2)

  // 3. Oversized long frame (declaring 128 MiB when limit is 64 MiB)
  let dec3 = FrameDecoder::new()
  let mut caught3 = false
  let _ = dec3.feed(b"\x02\x00\x00\x00\x00\x08\x00\x00\x00") catch {
    FrameTooLarge(_) => {
      caught3 = true
      []
    }
    _ => []
  }
  assert_true(caught3)

  // 4. Wrapped 64-bit length (0xFFFFFFFFFFFFFFFF)
  let dec4 = FrameDecoder::new()
  let mut caught4 = false
  let _ = dec4.feed(b"\x02\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF") catch {
    FrameTooLarge(_) => {
      caught4 = true
      []
    }
    _ => []
  }
  assert_true(caught4)
}

///|
test "greeting version negotiation and libzmq padding compatibility" {
  let g = Greeting::new(mechanism="NULL", as_server=false)
  let wire = g.encode()
  assert_eq(wire.length(), 64)
  assert_eq(wire[0].to_int(), 0xFF)
  assert_eq(wire[8].to_int(), 0x01)
  assert_eq(wire[9].to_int(), 0x7F)
  assert_eq(wire[10].to_int(), 3)
  assert_eq(wire[11].to_int(), 1)
  let parsed = Greeting::decode(wire)
  assert_eq(parsed, g)

  // ZMTP 3.0 peer greeting with non-zero padding bytes
  let raw30 : Array[Byte] = []
  for i = 0; i < 64; i = i + 1 {
    if i == 3 {
      raw30.push(b'\xAA')
    } else if i == 11 {
      raw30.push(b'\x00') // minor = 0 (ZMTP 3.0)
    } else {
      raw30.push(wire[i])
    }
  }
  let parsed30 = Greeting::decode(Bytes::from_array(raw30))
  assert_eq(parsed30.version_major, 3)
  assert_eq(parsed30.version_minor, 0)
  assert_eq(parsed30.mechanism, "NULL")

  // ZMTP 2.0 peer greeting must be rejected
  let raw20 : Array[Byte] = []
  for i = 0; i < 64; i = i + 1 {
    raw20.push(if i == 10 { b'\x02' } else { wire[i] })
  }
  let mut rejected = false
  let _ = Greeting::decode(Bytes::from_array(raw20)) catch {
    ProtocolError(_) => {
      rejected = true
      g
    }
    _ => g
  }
  assert_true(rejected)
}

///|
test "READY metadata with Socket-Type, Identity, and case-insensitive keys" {
  let meta = Metadata::new(
    "ROUTER",
    identity=Some(b"worker-01"),
    properties=[("X-Custom", b"value-123")],
  )
  let cmd_wire = ZmtpCommand::Ready(meta).encode_frame()
  let dec = FrameDecoder::new()
  let items = dec.feed(cmd_wire)
  assert_eq(items.length(), 1)
  match items[0] {
    CommandFrame(body) => {
      let decoded_cmd = ZmtpCommand::decode_body(body)
      match decoded_cmd {
        Ready(m) => {
          assert_eq(m.socket_type, "ROUTER")
          assert_eq(m.identity, Some(b"worker-01"))
          assert_eq(m.get("socket-type"), Some(b"ROUTER"))
          assert_eq(m.get("identity"), Some(b"worker-01"))
          assert_eq(m.get("x-custom"), Some(b"value-123"))
        }
        _ => assert_true(false)
      }
    }
    _ => assert_true(false)
  }
}

///|
test "ERROR, PING, PONG, SUBSCRIBE, and CANCEL commands" {
  let commands : Array[ZmtpCommand] = [
    ErrorCmd("Socket type not allowed"),
    Ping(500, b"hb-ctx"),
    Pong(b"hb-ctx"),
    Subscribe(b"market.btc"),
    Cancel(b"market.btc"),
  ]
  let dec = FrameDecoder::new()
  for c in commands {
    let wire = c.encode_frame()
    let items = dec.feed(wire)
    assert_eq(items.length(), 1)
    match items[0] {
      CommandFrame(body) => assert_eq(ZmtpCommand::decode_body(body), c)
      _ => assert_true(false)
    }
  }
}

///|
test "NullSession full handshake, incompatible socket type ERROR, and PING/PONG" {
  let client = NullSession::new(Req, local_identity=Some(b"req-client-1"))
  let server = NullSession::new(Rep)

  // Feed client greeting + READY to server
  let c_init = concat_bytes([
    client.initial_greeting(),
    client.local_ready_frame(),
  ])
  let s_events = server.feed(c_init)
  // Server emits SendBytes(READY) and HandshakeReady(client_meta)
  assert_eq(s_events.length(), 2)
  assert_eq(
    s_events[1],
    HandshakeReady(Metadata::new("REQ", identity=Some(b"req-client-1"))),
  )

  // Feed server greeting + READY to client byte-by-byte
  let s_init = concat_bytes([
    server.initial_greeting(),
    server.local_ready_frame(),
  ])
  let mut c_ready = false
  for i = 0; i < s_init.length(); i = i + 1 {
    for ev in client.feed(slice_bytes(s_init, i, i + 1)) {
      if ev == HandshakeReady(Metadata::new("REP")) {
        c_ready = true
      }
    }
  }
  assert_true(c_ready)

  // Exchange PING -> PONG and multipart message
  let ping_wire = ZmtpCommand::Ping(10, b"keepalive").encode_frame()
  let msg = Message::multipart([b"", b"hello", Bytes::make(400, b'X')])
  let combined = concat_bytes([ping_wire, msg.encode()])
  let s_ev2 = server.feed(combined)
  assert_eq(s_ev2.length(), 3)
  assert_eq(
    s_ev2[0],
    SendBytes(ZmtpCommand::Pong(b"keepalive").encode_frame()),
  )
  assert_eq(s_ev2[1], CommandReceived(Ping(10, b"keepalive")))
  assert_eq(s_ev2[2], MessageReceived(msg))

  // Incompatible socket types: REQ connecting to PUB must emit ERROR command and fail
  let bad_peer = NullSession::new(Pub)
  let bad_wire = concat_bytes([
    bad_peer.initial_greeting(),
    bad_peer.local_ready_frame(),
  ])
  let err_client = NullSession::new(Req)
  let bad_events = err_client.feed(bad_wire)
  let mut emitted_error_frame = false
  let mut failed_handshake = false
  for ev in bad_events {
    match ev {
      SendBytes(raw) => {
        let dec = FrameDecoder::new()
        let items = dec.feed(raw)
        if items.length() == 1 {
          match items[0] {
            CommandFrame(b) =>
              match ZmtpCommand::decode_body(b) {
                ErrorCmd(_) => emitted_error_frame = true
                _ => ()
              }
            _ => ()
          }
        }
      }
      ProtocolFailure(_) => failed_handshake = true
      _ => ()
    }
  }
  assert_true(emitted_error_frame)
  assert_true(failed_handshake)
}

///|
fn next_prng(state : Int) -> Int {
  // Deterministic 31-bit LCG for reproducible property-based testing
  (state * 1103515245 + 12345) & 0x7FFFFFFF
}

///|
test "property test: random multipart messages encode/decode roundtrip and arbitrary chunking" {
  let mut seed = 424242
  for round = 0; round < 50; round = round + 1 {
    seed = next_prng(seed)
    let msg_count = 1 + seed % 8
    let expected_messages : Array[Message] = []
    let wire_parts : Array[Bytes] = []
    for _m = 0; _m < msg_count; _m = _m + 1 {
      seed = next_prng(seed)
      let frame_count = 1 + seed % 6
      let frames : Array[Bytes] = []
      for _f = 0; _f < frame_count; _f = _f + 1 {
        seed = next_prng(seed)
        // Mix short frames (0..255) and long frames (256..1024)
        let flen = if seed % 4 == 0 {
          256 + (seed >> 3) % 768
        } else {
          (seed >> 3) % 256
        }
        let fbytes : Array[Byte] = Array(capacity=flen)
        for i = 0; i < flen; i = i + 1 {
          seed = next_prng(seed)
          fbytes.push((seed & 0xFF).to_byte())
        }
        frames.push(Bytes::from_array(fbytes))
      }
      let msg = Message::multipart(frames)
      expected_messages.push(msg)
      wire_parts.push(msg.encode())
    }
    let full_wire = concat_bytes(wire_parts)

    // Property 1: Full buffer decode equals original messages
    let dec_full = FrameDecoder::new()
    let full_items = dec_full.feed(full_wire)
    assert_eq(full_items.length(), expected_messages.length())
    for i = 0; i < expected_messages.length(); i = i + 1 {
      assert_eq(full_items[i], DataMessage(expected_messages[i]))
    }

    // Property 2: Random arbitrary chunk sizes (from 1 byte up to 37 bytes) reassemble identically
    let dec_chunked = FrameDecoder::new()
    let reassembled : Array[DecodedItem] = []
    let mut pos = 0
    while pos < full_wire.length() {
      seed = next_prng(seed)
      let step = 1 + seed % 37
      let end = if pos + step > full_wire.length() {
        full_wire.length()
      } else {
        pos + step
      }
      let slice = slice_bytes(full_wire, pos, end)
      for item in dec_chunked.feed(slice) {
        reassembled.push(item)
      }
      pos = end
    }
    assert_eq(reassembled.length(), expected_messages.length())
    for i = 0; i < expected_messages.length(); i = i + 1 {
      assert_eq(reassembled[i], DataMessage(expected_messages[i]))
    }
  }
}
