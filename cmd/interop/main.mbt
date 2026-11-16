///|
/// Comprehensive interop test binary supporting all ZMTP 3.1 socket patterns
/// and security mechanisms to verify bidirectional interoperability with libzmq/pyzmq.

async fn main {
  let matches = @argparse.Command(
    "interop",
    about="MoonBit ZeroMQ interop test runner",
    positionals=[
      @argparse.PositionArg(
        "cmd",
        about="command and arguments",
        num_args=@argparse.ValueRange(lower=0),
        allow_hyphen_values=true,
      ),
    ],
  ).parse() catch {
    _ => return
  }
  let args = match matches.values.get("cmd") {
    Some(list) => list
    None => []
  }
  if args.is_empty() {
    println("Usage: interop <mode> <port>")
    return
  }
  let mode = args[0]
  let port = if args.length() > 1 { args[1] } else { "5555" }
  let endpoint = "tcp://127.0.0.1:\{port}"

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()
    match mode {
      "req_server" => {
        let sock = ctx.socket(@moon_zmq.Rep) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let req = sock.recv() catch { _ => return }
        let req_str = req.first_string()
        println("RECV:\{req_str}")
        let _ = sock.send(@moon_zmq.Message::from_string("echo:\{req_str}")) catch { _ => return }
        println("SENT")
        @async.sleep(50)
        sock.close()
      }
      "req_client" => {
        let sock = ctx.socket(@moon_zmq.Req) catch { _ => return }
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        let _ = sock.send(@moon_zmq.Message::from_string("hello_from_mbt")) catch { _ => return }
        let rep = sock.recv() catch { _ => return }
        println("REPLY:\{rep.first_string()}")
        sock.close()
      }
      "pub_server" => {
        let sock = ctx.socket(@moon_zmq.Pub) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        // Allow subscriber to connect and establish ZMTP handshake
        while sock.active_peer_count() < 1 {
          @async.sleep(20)
        }
        @async.sleep(100)
        let _ = sock.send(@moon_zmq.Message::from_string("alerts.disk Disk full")) catch { _ => return }
        let _ = sock.send(@moon_zmq.Message::from_string("metrics.cpu 85")) catch { _ => return }
        println("PUBLISHED")
        @async.sleep(100)
        sock.close()
      }
      "sub_client" => {
        let sock = ctx.socket(@moon_zmq.Sub) catch { _ => return }
        sock.subscribe(b"alerts")
        sock.connect(group, endpoint)
        println("READY")
        let msg = sock.recv() catch { _ => return }
        println("SUB_RECV:\{msg.first_string()}")
        sock.close()
      }
      "push_server" => {
        let sock = ctx.socket(@moon_zmq.Push) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        while sock.active_peer_count() < 1 {
          @async.sleep(20)
        }
        @async.sleep(50)
        let _ = sock.send(@moon_zmq.Message::from_string("task_data_42")) catch { _ => return }
        println("PUSHED")
        @async.sleep(50)
        sock.close()
      }
      "pull_client" => {
        let sock = ctx.socket(@moon_zmq.Pull) catch { _ => return }
        sock.connect(group, endpoint)
        println("READY")
        let msg = sock.recv() catch { _ => return }
        println("PULLED:\{msg.first_string()}")
        sock.close()
      }
      "pull_server" => {
        let sock = ctx.socket(@moon_zmq.Pull) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let msg = sock.recv() catch { _ => return }
        println("PULLED:\{msg.first_string()}")
        sock.close()
      }
      "push_client" => {
        let sock = ctx.socket(@moon_zmq.Push) catch { _ => return }
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        let _ = sock.send(@moon_zmq.Message::from_string("task_from_mbt_push")) catch { _ => return }
        println("PUSHED")
        @async.sleep(50)
        sock.close()
      }
      "router_server" => {
        let sock = ctx.socket(@moon_zmq.Router) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let msg = sock.recv() catch { _ => return }
        // msg contains [router_id, empty_delim, payload] or [router_id, payload]
        let routing_id = msg.frames[0]
        let payload = if msg.frames.length() > 2 {
          msg.frames[2]
        } else {
          msg.frames[1]
        }
        let payload_str = @moon_zmq.ascii_bytes_to_string(payload)
        println("ROUTER_RECV:\{payload_str}")
        let reply_frames : Array[Bytes] = [
          routing_id,
          b"",
          @moon_zmq.string_to_ascii_bytes("router_echo:\{payload_str}"),
        ]
        let _ = sock.send(@moon_zmq.Message::multipart(reply_frames)) catch { _ => return }
        println("ROUTER_SENT")
        @async.sleep(50)
        sock.close()
      }
      "dealer_client" => {
        let sock = ctx.socket(@moon_zmq.Dealer) catch { _ => return }
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        let req_frames : Array[Bytes] = [b"", b"dealer_msg"]
        let _ = sock.send(@moon_zmq.Message::multipart(req_frames)) catch { _ => return }
        let rep = sock.recv() catch { _ => return }
        // Last frame is payload
        let last_frame = rep.frames[rep.frames.length() - 1]
        println("DEALER_RECV:\{@moon_zmq.ascii_bytes_to_string(last_frame)}")
        sock.close()
      }
      "pair_server" => {
        let sock = ctx.socket(@moon_zmq.Pair) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let msg = sock.recv() catch { _ => return }
        let msg_str = msg.first_string()
        println("PAIR_RECV:\{msg_str}")
        let _ = sock.send(@moon_zmq.Message::from_string("pair_pong:\{msg_str}")) catch { _ => return }
        @async.sleep(50)
        sock.close()
      }
      "pair_client" => {
        let sock = ctx.socket(@moon_zmq.Pair) catch { _ => return }
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        let _ = sock.send(@moon_zmq.Message::from_string("pair_ping")) catch { _ => return }
        let rep = sock.recv() catch { _ => return }
        println("PAIR_REPLY:\{rep.first_string()}")
        sock.close()
      }
      "multipart_server" => {
        let sock = ctx.socket(@moon_zmq.Rep) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let req = sock.recv() catch { _ => return }
        println("MP_FRAMES:\{req.frames.length()}")
        // Echo frames in reverse order
        let rev : Array[Bytes] = []
        let mut i = req.frames.length() - 1
        while i >= 0 {
          rev.push(req.frames[i])
          i = i - 1
        }
        let _ = sock.send(@moon_zmq.Message::multipart(rev)) catch { _ => return }
        @async.sleep(50)
        sock.close()
      }
      "multipart_client" => {
        let sock = ctx.socket(@moon_zmq.Req) catch { _ => return }
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        let frames : Array[Bytes] = [b"part1", b"part2", b"part3"]
        let _ = sock.send(@moon_zmq.Message::multipart(frames)) catch { _ => return }
        let rep = sock.recv() catch { _ => return }
        println("MP_REPLY_FRAMES:\{rep.frames.length()}")
        for f in rep.frames {
          println("MP_FRAME:\{@moon_zmq.ascii_bytes_to_string(f)}")
        }
        sock.close()
      }
      "big_server" => {
        let sock = ctx.socket(@moon_zmq.Rep) catch { _ => return }
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let req = sock.recv() catch { _ => return }
        let total_len = req.frames[0].length()
        println("BIG_RECV_LEN:\{total_len}")
        let _ = sock.send(@moon_zmq.Message::from_string("big_ack:\{total_len}")) catch { _ => return }
        @async.sleep(50)
        sock.close()
      }
      "big_client" => {
        let sock = ctx.socket(@moon_zmq.Req) catch { _ => return }
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        // Construct 512KB payload
        let size = 524288
        let buf : Array[Byte] = Array(capacity=size)
        for i = 0; i < size; i = i + 1 {
          buf.push(b'A')
        }
        let _ = sock.send(@moon_zmq.Message::from_bytes(Bytes::from_array(buf))) catch { _ => return }
        let rep = sock.recv() catch { _ => return }
        println("BIG_REPLY:\{rep.first_string()}")
        sock.close()
      }
      "plain_server" => {
        let sock = ctx.socket(@moon_zmq.Rep) catch { _ => return }
        sock.set_plain_server(
          true,
          authenticator=Some(fn(u, p) { u == "admin" && p == "secret123" }),
        )
        let _ = sock.bind(group, endpoint) catch { _ => return }
        println("READY")
        let req = sock.recv() catch { _ => return }
        println("PLAIN_RECV:\{req.first_string()}")
        let _ = sock.send(@moon_zmq.Message::from_string("plain_auth_ok")) catch { _ => return }
        @async.sleep(50)
        sock.close()
      }
      "plain_client" => {
        let sock = ctx.socket(@moon_zmq.Req) catch { _ => return }
        sock.set_plain_credentials("admin", "secret123")
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        let _ = sock.send(@moon_zmq.Message::from_string("secure_hello")) catch { _ => return }
        let rep = sock.recv() catch { _ => return }
        println("PLAIN_REPLY:\{rep.first_string()}")
        sock.close()
      }
      "reconnect_client" => {
        let sock = ctx.socket(@moon_zmq.Req) catch { _ => return }
        sock.set_reconnect_interval(100, max_ms=500)
        sock.connect(group, endpoint)
        while sock.active_peer_count() < 1 {
          @async.sleep(10)
        }
        // Request 1
        let _ = sock.send(@moon_zmq.Message::from_string("req_1")) catch { _ => return }
        let rep1 = sock.recv() catch { _ => return }
        println("REPLY1:\{rep1.first_string()}")

        // Wait for server to restart
        @async.sleep(1000)
        while sock.active_peer_count() < 1 {
          @async.sleep(50)
        }

        // Request 2
        let _ = sock.send(@moon_zmq.Message::from_string("req_2")) catch { _ => return }
        let rep2 = sock.recv() catch { _ => return }
        println("REPLY2:\{rep2.first_string()}")
        sock.close()
      }
      _ => println("Unknown mode: \{mode}")
    }
    ctx.term()
  })
}
