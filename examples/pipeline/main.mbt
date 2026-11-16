///|
/// Pipeline (PUSH/PULL) Task Distribution Example in MoonBit
/// Demonstrates high-throughput parallel compute dispatch and fair queuing worker collection.

async fn main {
  println("=== ZeroMQ PUSH/PULL Pipeline Example in MoonBit ===")

  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. Task Collector / Sink (PULL)
    let sink = ctx.socket(@moon_zmq.Pull) catch {
      err => {
        println("Failed to create sink: \{err}")
        return
      }
    }
    let endpoint = sink.bind(group, "tcp://127.0.0.1:0") catch {
      err => {
        println("Failed to bind sink: \{err}")
        return
      }
    }
    println("[Sink] Collecting work batches on \{endpoint}")

    // 2. Task Producer (PUSH)
    let producer = ctx.socket(@moon_zmq.Push) catch {
      err => {
        println("Failed to create producer: \{err}")
        return
      }
    }
    producer.connect(group, endpoint)

    while sink.active_peer_count() < 1 || producer.active_peer_count() < 1 {
      @async.sleep(10)
    }

    // 3. Dispatch batch of 5 jobs
    let job_count = 5
    println("[Producer] Dispatching \{job_count} computational jobs...")
    for i = 1; i <= job_count; i = i + 1 {
      let job_payload = "compute_job_#\{i}: matrix_mult(dim=1024)"
      let _ = producer.send(@moon_zmq.Message::from_string(job_payload)) catch {
        err => {
          println("Producer send failed: \{err}")
          return
        }
      }
      println("  -> Dispatched: '\{job_payload}'")
    }

    // 4. Sink collects jobs via fair-queuing
    println("[Sink] Processing received jobs:")
    for i = 1; i <= job_count; i = i + 1 {
      let job = sink.recv() catch {
        err => {
          println("Sink recv failed: \{err}")
          return
        }
      }
      println("  <- Collected: '\{job.first_string()}'")
    }

    producer.close()
    sink.close()
    ctx.term()
    println("=== Pipeline Completed Successfully ===")
  })
}
