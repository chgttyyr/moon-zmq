///|
/// Comprehensive throughput and latency benchmarks for pure MoonBit ZeroMQ (moon_zmq).
/// Measures across standard message payload sizes: 64B, 1KB, 64KB, 1MB.

///|
fn make_benchmark_payload(size_bytes : Int32) -> Bytes {
  let arr : Array[Byte] = Array(capacity=size_bytes)
  for i = 0; i < size_bytes; i = i + 1 {
    arr.push(((i & 0x7F) + 0x20).to_byte())
  }
  Bytes::from_array(arr)
}

///|
async test "benchmark: push-pull pipeline throughput across payload sizes" {
  @async.with_task_group(async fn(group) {
    let pull = Socket::new(Pull)
    let ep = pull.bind(group, "tcp://127.0.0.1:0")

    let push = Socket::new(Push)
    push.connect(group, ep)

    while push.active_peer_count() < 1 || pull.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // Benchmark tiers: (name, size_bytes, count)
    let tiers : Array[(String, Int32, Int32)] = [
      ("64B", 64, 2000),
      ("1KB", 1024, 1000),
      ("64KB", 65536, 100),
      ("1MB", 1048576, 10),
    ]

    for tier in tiers {
      let (name, size, count) = tier
      let payload = make_benchmark_payload(size)

      let t_start = @async.now()

      // Spawn receiver task
      let recv_task = group.spawn(no_wait=true, allow_failure=true, async fn() {
        for _ in 0..<count {
          let _ = pull.recv()
        }
      })

      // Send loop
      for _ in 0..<count {
        push.send(Message::from_bytes(payload))
      }

      recv_task.wait()
      let t_end = @async.now()
      let duration_ms = if t_end - t_start <= 0L { 1L } else { t_end - t_start }
      let msgs_per_sec = count.to_int64() * 1000L / duration_ms
      let mb_transferred = (size.to_int64() * count.to_int64()).to_double() /
        (1024.0 * 1024.0)
      let mb_per_sec = mb_transferred * 1000.0 / duration_ms.to_double()

      println(
        "[BENCHMARK] PUSH/PULL " +
        name +
        ": " +
        "\{count} msgs in \{duration_ms} ms => \{msgs_per_sec} msg/s, " +
        "\{mb_per_sec} MB/s",
      )

      assert_true(msgs_per_sec > 0)
    }

    push.close()
    pull.close()
  })
}

///|
async test "benchmark: req-rep round-trip latency (p50, p95, p99)" {
  @async.with_task_group(async fn(group) {
    let rep = Socket::new(Rep)
    let ep = rep.bind(group, "tcp://127.0.0.1:0")

    let req = Socket::new(Req)
    req.connect(group, ep)

    while req.active_peer_count() < 1 || rep.active_peer_count() < 1 {
      @async.sleep(10)
    }

    let tiers : Array[(String, Int32, Int32)] = [
      ("64B", 64, 500),
      ("1KB", 1024, 200),
      ("64KB", 65536, 50),
      ("1MB", 1048576, 10),
    ]

    for tier in tiers {
      let (name, size, count) = tier
      let payload = make_benchmark_payload(size)

      // Background REP echo worker
      let worker_task = group.spawn(no_wait=true, allow_failure=true, async fn() {
        for _ in 0..<count {
          let msg = rep.recv()
          rep.send(msg)
        }
      })

      let samples : Array[Int64] = []
      let t_total_start = @async.now()

      for _ in 0..<count {
        let t0 = @async.now()
        req.send(Message::from_bytes(payload))
        let _ = req.recv()
        let t1 = @async.now()
        samples.push(t1 - t0)
      }

      worker_task.wait()
      let t_total_end = @async.now()
      let total_ms = if t_total_end - t_total_start <= 0L {
        1L
      } else {
        t_total_end - t_total_start
      }

      samples.sort()
      let p50 = samples[samples.length() * 50 / 100]
      let p95 = samples[samples.length() * 95 / 100]
      let p99 = samples[samples.length() * 99 / 100]
      let min_lat = samples[0]
      let max_lat = samples[samples.length() - 1]
      let avg_lat = total_ms.to_double() / count.to_double()

      println(
        "[BENCHMARK] REQ/REP " +
        name +
        " (n=\{count}): " +
        "min=\{min_lat}ms, p50=\{p50}ms, p95=\{p95}ms, p99=\{p99}ms, max=\{max_lat}ms, avg=\{avg_lat}ms",
      )

      assert_true(samples.length() == count)
    }

    req.close()
    rep.close()
  })
}
