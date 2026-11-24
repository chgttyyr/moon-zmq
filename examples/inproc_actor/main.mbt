///|
/// Inproc Zero-Copy Transport, ZActor, ZConfig (RFC 4) & Radio/Dish Example in MoonBit
async fn main {
  println("=== ZeroMQ inproc://, ZActor, ZConfig & Radio/Dish Example ===")

  let zpl_text =
    #|socket
    #|    sndhwm = 2048
    #|    rcvhwm = 2048
    #|    identity = "telemetry-actor"
  let cfg = @moon_zmq.ZConfig::parse(zpl_text) catch { _ => return }
  let opts = @moon_zmq.SocketOptions::new()
  @moon_zmq.apply_zconfig_to_socket_options(cfg, opts)
  let hwm = cfg.get_int("socket/sndhwm")
  println("[ZConfig] Loaded RFC 4 ZPL configuration: sndhwm=\{hwm}")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()
    defer ctx.term()

    // 1. Spawn a CZMQ-style ZActor over an automatic inproc:// PAIR pipe
    let actor = @moon_zmq.ZActor::spawn(group, ctx, async fn(pipe) {
      while true {
        let cmd = pipe.recv_string() catch { _ => break }
        if cmd == @moon_zmq.ZACTOR_TERM_CMD {
          break
        }
        let z85_tag = @moon_zmq.z85_encode_padded(
          @moon_zmq.string_to_ascii_bytes(cmd),
        ) catch {
          _ => ""
        }
        pipe.send_string("Z85(\{z85_tag})") catch {
          _ => break
        }
      }
    }) catch {
      _ => return
    }

    actor.send_string("MoonBit-ZMQ") catch {
      _ => return
    }
    let encoded_reply = actor.recv_string() catch { _ => return }
    println("[ZActor] Processed over inproc:// PAIR -> \{encoded_reply}")
    actor.stop()

    // 2. Radio / Dish exact-group pub-sub over inproc://
    let radio = @moon_zmq.RadioSocket::new(ctx, options=opts) catch {
      _ => return
    }
    let dish = @moon_zmq.DishSocket::new(ctx) catch { _ => return }
    let _ = radio.bind(group, "inproc://cluster-events") catch { _ => return }
    dish.join("cluster.leader") catch {
      _ => return
    }
    dish.connect(group, "inproc://cluster-events") catch {
      _ => return
    }
    @async.sleep(25)

    radio.send_group_string("cluster.leader.debug", "ignored-subgroup") catch {
      _ => return
    }
    radio.send_group_string("cluster.leader", "node-01 elected primary") catch {
      _ => return
    }

    let (grp_name, msg) = dish.recv_group() catch { _ => return }
    let body = msg.first_string()
    println("[Radio/Dish] Received group='\{grp_name}', payload='\{body}'")
  })

  println("=== Inproc Actor & Radio/Dish Example Completed Successfully ===")
}
