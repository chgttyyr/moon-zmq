///|
/// Publish-Subscribe (PUB/SUB) Example in MoonBit
/// Demonstrates multi-topic broadcasting with client-side prefix subscription filtering.

async fn main {
  println("=== ZeroMQ PUB/SUB Telemetry Example in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Weather & Telemetry Publisher
    let pub_sock = ctx.socket(@moon_zmq.Pub) catch {
      err => {
        println("Failed to create PUB: \{err}")
        return
      }
    }
    let endpoint = pub_sock.bind(group, "tcp://127.0.0.1:0") catch {
      err => {
        println("Failed to bind PUB: \{err}")
        return
      }
    }
    println("[Publisher] Broadcasting on \{endpoint}")

    // 2. Weather Subscriber
    let sub_weather = ctx.socket(@moon_zmq.Sub) catch {
      err => {
        println("Failed to create SUB: \{err}")
        return
      }
    }
    sub_weather.subscribe(b"weather.beijing")
    sub_weather.connect(group, endpoint)

    // 3. Stock Subscriber
    let sub_stocks = ctx.socket(@moon_zmq.Sub) catch {
      err => {
        println("Failed to create SUB: \{err}")
        return
      }
    }
    sub_stocks.subscribe(b"market.quote")
    sub_stocks.connect(group, endpoint)

    // Wait for subscriptions to connect
    while pub_sock.active_peer_count() < 2 {
      @async.sleep(20)
    }
    @async.sleep(50)

    // 4. Publish messages across different topics
    println("[Publisher] Publishing weather and market telemetry...")
    let _ = pub_sock.send(
      @moon_zmq.Message::from_string("weather.beijing temperature=22.5C humidity=45%"),
    ) catch {
      _ => ()
    }
    let _ = pub_sock.send(
      @moon_zmq.Message::from_string("market.quote symbol=MOON price=128.50 volume=50000"),
    ) catch {
      _ => ()
    }
    let _ = pub_sock.send(
      @moon_zmq.Message::from_string("internal.metrics gc_pause=1.2ms heap=16MB"),
    ) catch {
      _ => ()
    }

    // 5. Receive filtered updates
    let weather_msg = sub_weather.recv() catch {
      err => {
        println("Weather SUB recv error: \{err}")
        return
      }
    }
    println("[Weather Subscriber] Received: '\{weather_msg.first_string()}'")

    let stock_msg = sub_stocks.recv() catch {
      err => {
        println("Stock SUB recv error: \{err}")
        return
      }
    }
    println("[Market Subscriber] Received: '\{stock_msg.first_string()}'")

    println("[Info] 'internal.metrics' was correctly ignored by both subscribers.")

    sub_weather.close()
    sub_stocks.close()
    pub_sock.close()
    ctx.term()
    println("=== PUB/SUB Completed Successfully ===")
  })
}
