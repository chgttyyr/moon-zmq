# moon-zmq: 纯 MoonBit 实现的 ZeroMQ (ZMTP 3.1) 消息通信库

[![CI](https://github.com/chgttyyr/moon-zmq/actions/workflows/ci.yml/badge.svg)](https://github.com/chgttyyr/moon-zmq/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Target: Native](https://img.shields.io/badge/Target-Native-green.svg)](docs/design.md)

`moon-zmq` 是一个基于纯 MoonBit 语言实现的、工业级、通用的 ZeroMQ（ZMTP 3.1）网络通信库。它旨在为 MoonBit 生态提供高性能、全双工、跨语言互通的消息通信总线能力，能够直接与官方 C/C++ `libzmq` 以及各语言驱动（如 Python `pyzmq`、Rust `zmq`、Go `pebbe/zmq4` 等）进行原生无缝通信。

---

## 致谢与同类生态对比

在 MoonBit 生态早期，[`Mr-Shi123/moonjupyter`](https://github.com/Mr-Shi123/moonjupyter) 项目开创性地为 Jupyter 内核协议内嵌了一小段基础的 ZMTP NULL 握手与单向帧编解码逻辑，为本项目的初期协议调研提供了非常有价值的参考与启发，在此特向 `Mr-Shi123` 及其贡献者致以崇高的敬意与诚挚的感谢！

### 与 `moonjupyter` 内置实现的本质区别：

| 特性维度 | moonjupyter 内嵌实现 | moon-zmq (本项目) |
| :--- | :--- | :--- |
| **项目定位** | 专为 Jupyter Kernel 协议定制的内嵌辅助代码 | **独立、通用、可发布的现代 ZeroMQ 基础设施库** |
| **Socket 模式支持** | 仅满足 Jupyter 通信的基本收发，无标准模式状态机 | **完整覆盖 10 种标准模式**：REQ/REP、PUB/SUB、XPUB/XSUB、PUSH/PULL、DEALER/ROUTER、PAIR |
| **规范遵从度** | 基础握手，无严格 RFC 状态机校验 | **严格对标 RFC 23 / 24 / 28 / 29 / 30 / 31 / 47**，具备信封包装与交替检查 |
| **连接与高可用** | 单次连接，无重连与流量控制 | **多连接并发管理 (`TransportHub`)、指数退避自动重连、高水位 (HWM) 反压队列** |
| **安全机制** | 仅支持简易无认证的 NULL 机制 | **支持 NULL 机制与 RFC 24 PLAIN 明文认证机制**，支持自定义服务端 ZAP 校验回调 |
| **双向跨语言互通** | 未针对标准 libzmq 进行双向全面互通校验 | **具备与官方 libzmq (`pyzmq`) 的全量双向端到端互通测试套件 (18/18 通过)** |

---

## 核心特性

- **纯 MoonBit 原生构建**：基于现代协作式协程库 `moonbitlang/async` 编写，无 CGO、无外部动态链接库依赖。
- **高并发全异步 I/O**：深度利用操作系统原生异步事件机制（Windows IOCP、Linux epoll、macOS kqueue），通过结构化并发 `TaskGroup` 保证连接与资源安全释放。
- **全模式覆盖**：
  - **REQ / REP**：严格交替状态机与空帧信封（Empty Delimiter Frame）自动化管理；
  - **DEALER / ROUTER**：全双工并发收发与身份路由（Identity Routing），支持 `ZMQ_ROUTER_MANDATORY`；
  - **PUB / SUB & XPUB / XSUB**：多主题前缀订阅与本地过滤，支持构建层级发布订阅代理（Broker）；
  - **PUSH / PULL**：基于公平轮询（Round-Robin）与公平排队（Fair-Queuing）的高性能流水线分发；
  - **PAIR**：点对点排他低开销通信。
- **高级调度与中介组件**：
  - **RFC 43 可控消息代理 (`proxy`, `proxy_steerable`)**：支持全自动双向流量中继、旁路流量审计抓包（Capture）及控制通道动态暂停、恢复与终止（PAUSE / RESUME / TERMINATE）；
  - **I/O 多路复用调度器 (`Poller`)**：在单协程内统一轮询多套接字，非阻塞探测 `POLLIN` / `POLLOUT` / `POLLERR`；
  - **ZMTP 3.1 心跳检测 (`set_heartbeat`)**：支持定时 PING 探测、对端自动 PONG 响应与静默失活自动断连重连。
- **企业级连接管理**：
  - 自动重连机制（支持可配置的初始重连间隔与指数退避上限）；
  - 高水位标记（`sndhwm` / `rcvhwm`）反压队列与丢弃策略；
  - 动态多连接管理与管道隔离。
- **完备的安全性**：
  - 支持 RFC 23 NULL 机制；
  - 支持 RFC 24 PLAIN 机制，含服务端可配置的 ZAP（ZeroMQ Authentication Protocol）认证处理器。

---

## 安装与引入

在您的 MoonBit 项目根目录下的 `moon.mod` 或 `moon.pkg` 中添加依赖：

```moonbit
import {
  "chgttyyr/moon_zmq",
  "moonbitlang/core/async",
}
```

或者使用 `moon` 包管理命令添加：

```bash
moon add chgttyyr/moon_zmq
```

> **注意**：`moon-zmq` 需在 native 目标下编译运行（`--target native`）。

---

## 快速开始

### 1. 基础请求-应答模式 (REQ / REP)

```moonbit
async fn run_req_rep() -> Unit {
  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. 服务端监听
    let rep = ctx.socket(@moon_zmq.Rep) catch { _ => return }
    let endpoint = rep.bind(group, "tcp://127.0.0.1:5555") catch { _ => return }

    // 2. 客户端连接
    let req = ctx.socket(@moon_zmq.Req) catch { _ => return }
    req.connect(group, endpoint)

    // 等待建立握手
    while rep.active_peer_count() < 1 { @async.sleep(10) }

    // 3. 客户端发送请求
    let _ = req.send(@moon_zmq.Message::from_string("ping")) catch { _ => return }

    // 4. 服务端接收并应答
    let request = rep.recv() catch { _ => return }
    println("Server received: \{request.first_string()}")
    let _ = rep.send(@moon_zmq.Message::from_string("pong")) catch { _ => return }

    // 5. 客户端获取响应
    let reply = req.recv() catch { _ => return }
    println("Client received: \{reply.first_string()}")

    req.close()
    rep.close()
    ctx.term()
  })
}
```

### 2. 发布-订阅遥测与过滤 (PUB / SUB)

```moonbit
async fn run_pub_sub() -> Unit {
  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()
    let pub_sock = ctx.socket(@moon_zmq.Pub) catch { _ => return }
    let endpoint = pub_sock.bind(group, "tcp://127.0.0.1:0") catch { _ => return }

    let sub_sock = ctx.socket(@moon_zmq.Sub) catch { _ => return }
    // 订阅指定主题前缀
    sub_sock.subscribe(b"market.quote")
    sub_sock.connect(group, endpoint)

    while pub_sock.active_peer_count() < 1 { @async.sleep(10) }
    @async.sleep(50)

    // 发布主题数据
    let _ = pub_sock.send(@moon_zmq.Message::from_string("market.quote symbol=MOON price=128.5")) catch { _ => () }
    let _ = pub_sock.send(@moon_zmq.Message::from_string("unrelated.topic ignored")) catch { _ => () }

    // 订阅端只会收到匹配订阅的主题
    let msg = sub_sock.recv() catch { _ => return }
    println("Received quote: \{msg.first_string()}")

    sub_sock.close()
    pub_sock.close()
    ctx.term()
  })
}
```

### 3. PLAIN 用户名密码安全认证 (RFC 24)

```moonbit
async fn run_secure_auth() -> Unit {
  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. 服务端配置 PLAIN 及认证回调
    let server = ctx.socket(@moon_zmq.Rep) catch { _ => return }
    server.set_plain_server(true, authenticator=Some(fn(user, pass) {
      user == "admin" && pass == "secret123"
    }))
    let endpoint = server.bind(group, "tcp://127.0.0.1:0") catch { _ => return }

    // 2. 客户端配置认证凭据
    let client = ctx.socket(@moon_zmq.Req) catch { _ => return }
    client.set_plain_credentials("admin", "secret123")
    client.connect(group, endpoint)

    while server.active_peer_count() < 1 { @async.sleep(10) }

    let _ = client.send(@moon_zmq.Message::from_string("secure rpc payload")) catch { _ => return }
    let req = server.recv() catch { _ => return }
    let _ = server.send(@moon_zmq.Message::from_string("auth confirmed ok")) catch { _ => return }
    let rep = client.recv() catch { _ => return }
    println("Secure Reply: \{rep.first_string()}")

    client.close()
    server.close()
    ctx.term()
  })
}
```

### 4. RFC 43 可控消息代理与旁路抓包 (Proxy & Capture)

```moonbit
async fn run_proxy_broker() -> Unit {
  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()

    // 1. 设置前端 ROUTER 与后端 DEALER
    let frontend = ctx.socket(@moon_zmq.Router) catch { _ => return }
    let f_ep = frontend.bind(group, "tcp://127.0.0.1:5559") catch { _ => return }

    let backend = ctx.socket(@moon_zmq.Dealer) catch { _ => return }
    let b_ep = backend.bind(group, "tcp://127.0.0.1:5560") catch { _ => return }

    // 2. 启动代理转发任务（自动双向转发并输出审计日志）
    let stats = @moon_zmq.ProxyStats::new()
    group.spawn(no_wait=true, allow_failure=true, async fn() {
      let _ = @moon_zmq.proxy_steerable(frontend, backend, stats~) catch { _ => () }
    })

    // 客户端连接 frontend (5559)，Worker 连接 backend (5560)，实现解耦路由
  })
}
```

### 5. I/O 多路复用调度 (Poller Multiplexing)

```moonbit
async fn run_poller_multiplex() -> Unit {
  @async.with_task_group(async fn(group) {
    let ctx = @moon_zmq.Context::new()
    let sock1 = ctx.socket(@moon_zmq.Pull) catch { _ => return }
    let sock2 = ctx.socket(@moon_zmq.Pair) catch { _ => return }

    // 注册到 Poller
    let poller = @moon_zmq.Poller::new()
    poller.add(sock1, @moon_zmq.POLLIN)
    poller.add(sock2, @moon_zmq.POLLIN)

    // 非阻塞超时轮询
    let ready_count = poller.poll(timeout_ms=50) catch { _ => 0 }
    if poller.has_in(sock1) {
      let msg = sock1.recv() catch { _ => return }
      println("Channel 1 event: \{msg.first_string()}")
    }
  })
}
```

---

## 规范覆盖表

`moon-zmq` 严格对齐 ZeroMQ RFC 官方标准：

| 规范 | 描述 | 状态 | 详细说明 |
| :--- | :--- | :---: | :--- |
| **RFC 23** | ZMTP 3.1 协议核心规范 | **100% 完成** | 支持短帧、长帧、`MORE` 标志、命令帧、版本协商与 NULL 握手 |
| **RFC 24** | PLAIN 安全机制 | **100% 完成** | 完整覆盖 HELLO/WELCOME/INITIATE 交互流与 ZAP 认证回调 |
| **RFC 25** | CURVE 安全机制 | **暂未实现** | 详细技术评估参见 [docs/spec-coverage.md](docs/spec-coverage.md)（等待生态标准库） |
| **RFC 28** | REQ / REP 套接字规范 | **100% 完成** | 状态机交替校验、空帧信封（Empty Delimiter）自动封装与解包 |
| **RFC 29** | PUB / SUB 套接字规范 | **100% 完成** | 多主题前缀匹配过滤与多连接广播分发 |
| **RFC 30** | PIPELINE (PUSH / PULL) 规范 | **100% 完成** | 公平轮询（Round-Robin）分发与公平排队（Fair-Queuing）汇聚 |
| **RFC 31** | ROUTER 套接字规范 | **100% 完成** | 异步多路复用、源节点 Identity 信封附加与目标寻址路由 |
| **RFC 32** | DEALER 套接字规范 | **100% 完成** | 全双工异步点对点轮询负载分发与接收 |
| **RFC 43** | ZeroMQ 可控代理与转发中介 | **100% 完成** | 双向流量中继、流量统计、可选旁路抓包与运行时 PAUSE/RESUME/TERMINATE 管控 |
| **RFC 47** | XPUB / XSUB 扩展发布订阅 | **100% 完成** | 支持下游订阅/取消订阅指令向上游透明中继 |
| **RFC 37** | ZMTP 心跳保活机制 | **100% 完成** | 周期 PING 探测、对端自动应答 PONG 与失活超时自动重连 |
| **N/A** | Poller 多路 I/O 复用器 | **100% 完成** | 单协程统一非阻塞轮询与超时探测 POLLIN / POLLOUT / POLLERR |
| **N/A** | PAIR 点对点独占模式 | **100% 完成** | 独占双向通道，零冗余信封开销 |

完整逐条规范对标说明与技术分析，请参见面向开发者的正式文档：[docs/spec-coverage.md](docs/spec-coverage.md)。

---

## 跨实现互通测试结果

本项目通过 Python 官方 `pyzmq`（底层驱动为官方 `libzmq 4.3.5`）进行了高标准的全量双向端到端互通测试（集成测试源码位于 `tests/interop/test_interop.py`），并在 GitHub Actions CI 中自动化运行：

```
test_big_message_moonbit_server (MoonBit REP 服务端接收 Python 512KB 大消息) ... ok
test_big_message_python_server (Python REP 服务端接收 MoonBit 512KB 大消息) ... ok
test_multipart_moonbit_server (MoonBit 接收 Python 多帧消息并倒序回复) ... ok
test_multipart_python_server (Python 接收 MoonBit 多帧消息并倒序回复) ... ok
test_pair_moonbit_server (MoonBit PAIR 服务端 <-> Python PAIR 客户端) ... ok
test_pair_python_server (Python PAIR 服务端 <-> MoonBit PAIR 客户端) ... ok
test_plain_auth_moonbit_server (MoonBit PLAIN 服务端验证 Python 认证凭据) ... ok
test_plain_auth_python_server (Python ThreadAuthenticator 服务端验证 MoonBit 凭据) ... ok
test_pub_sub_moonbit_server (MoonBit PUB 广播 <-> Python SUB 过滤订阅) ... ok
test_pub_sub_python_server (Python PUB 广播 <-> MoonBit SUB 过滤订阅) ... ok
test_pull_server_moonbit (MoonBit PULL 汇聚 <-> Python PUSH 生产) ... ok
test_push_pull_moonbit_server (MoonBit PUSH 分发 <-> Python PULL 接收) ... ok
test_push_pull_python_server (Python PUSH 分发 <-> MoonBit PULL 接收) ... ok
test_reconnection_moonbit_client (服务端崩溃重启后 MoonBit 客户端自动重连并恢复通信) ... ok
test_req_rep_moonbit_server (MoonBit REP 服务端 <-> Python REQ 客户端) ... ok
test_req_rep_python_server (Python REP 服务端 <-> MoonBit REQ 客户端) ... ok
test_router_dealer_moonbit_server (MoonBit ROUTER 代理 <-> Python DEALER 客户端) ... ok
test_router_dealer_python_server (Python ROUTER 服务端 <-> MoonBit DEALER 客户端) ... ok

----------------------------------------------------------------------
Ran 18 tests in 6.678s

OK (18 passed, 0 failed)
```

- **MoonBit 单元/状态机/属性/边界测试**：39/39 测试通过。
- **官方 libzmq 双向互通与基准测试**：20/20 测试通过。

---

## 真实基准性能测试数据 (Benchmarks)

在本地原生 TCP 回环环境（Windows x86_64，Intel Core 处理器）下对 `moon-zmq` 开展系统性基准性能压测，涵盖小包至超大包（64B ~ 1MB）的单向流水线吞吐量与双向 RPC 往返延迟分位数统计：

### 1. PUSH / PULL 单向流水线吞吐量

| 消息载荷规格 | 传输批次 | 耗时 | 消息吞吐率 (msg/s) | 带宽吞吐率 (MB/s) |
| :--- | :--- | :--- | :--- | :--- |
| **64 Bytes** | 2,000 帧 | 81 ms | **24,691 msg/s** | 1.51 MB/s |
| **1 KByte** | 1,000 帧 | 68 ms | **14,705 msg/s** | **14.36 MB/s** |
| **64 KBytes** | 100 帧 | 369 ms | 271 msg/s | **16.94 MB/s** |
| **1 MByte (超大帧)** | 10 帧 | 572 ms | 17 msg/s | **17.48 MB/s** |

> 注：在跨语言互通基准测试中（Python pyzmq 生产 -> MoonBit PULL 接收），64B 小包吞吐率达到 **47,965 msg/s**（2.93 MB/s），64KB 吞吐带宽达到 **27.20 MB/s**。

### 2. REQ / REP 双向往返延迟 (Round-Trip Latency Percentiles)

| 消息载荷规格 | 采样次数 (n) | Min | p50 (中位数) | p95 | p99 | Max | 平均延迟 (Avg) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 Bytes** | 500 次 | 0 ms | **0 ms** | **1 ms** | **1 ms** | 51 ms | **0.39 ms** |
| **1 KByte** | 200 次 | 0 ms | **0 ms** | **1 ms** | **2 ms** | 2 ms | **0.32 ms** |
| **64 KBytes** | 50 次 | 5 ms | **7 ms** | 20 ms | 88 ms | 88 ms | **9.58 ms** |
| **1 MByte** | 10 次 | 66 ms | **93 ms** | 139 ms | 139 ms | 139 ms | **93.1 ms** |

> 注：在官方 libzmq（pyzmq）与 MoonBit 互通延迟测试中，64B 小包 RPC 往返延迟 **p50 仅为 0.10 ms**（100 微秒），p99 为 0.20 ms；1KB 包往返延迟 **p50 仅为 0.12 ms**。

---

## 运行示例工程

仓库的 `examples/` 目录提供了 6 个开箱即用的完整独立示例工程：

```bash
# 1. 运行请求应答示例 (REQ / REP)
moon -C examples/req_rep run . --target native

# 2. 运行发布订阅遥测示例 (PUB / SUB)
moon -C examples/pub_sub run . --target native

# 3. 运行流水线任务分发示例 (PUSH / PULL)
moon -C examples/pipeline run . --target native

# 4. 运行路由代理中介示例 (ROUTER / DEALER)
moon -C examples/router_broker run . --target native

# 5. 运行 RFC 43 可控消息代理与旁路抓包示例 (Proxy Broker)
moon -C examples/proxy_broker run . --target native

# 6. 运行 Poller 多路事件复用轮询示例 (Poller Multiplex)
moon -C examples/poller_multiplex run . --target native
```

---

## 版本路线 (Roadmap)

- **v0.1.0 (已完成发布)**：
  - [x] ZMTP 3.1 帧编解码器、版本协商与 Greeting 握手；
  - [x] RFC 23 NULL 认证与 RFC 24 PLAIN 安全认证；
  - [x] 自动重连（指数退避）与 HWM 高水位反压队列；
  - [x] 10 种标准 Socket 模式（REQ/REP、PUB/SUB、XPUB/XSUB、PUSH/PULL、DEALER/ROUTER、PAIR）；
  - [x] 官方 libzmq (pyzmq) 双向互通自动化测试体系；
  - [x] 4 大基础可执行示例工程。
- **v0.2.0 (当前重大升级版本)**：
  - [x] RFC 43 可控消息代理与抓包转发器（`proxy`, `proxy_steerable`）；
  - [x] 基于 `Poller` 的多 Socket 统一异步 I/O 事件多路复用调度器；
  - [x] ZMTP 3.1 心跳检测（PING/PONG 周期探测与静默超时自动重连）；
  - [x] 完备的基准性能测试套件（吞吐率达 47,000+ msg/s，RPC 往返延迟达 0.10 ms）；
  - [x] 边界异常防御与故障注入测试套件（恶意包嗅探、高并发快速连接抖动防护）；
  - [x] 新增 2 大高级架构示例工程（`proxy_broker`, `poller_multiplex`）。
- **v1.0.0 (长期演进)**：
  - [ ] 待 MoonBit 生态正式发布经过审计的 Curve25519 库后，支持 RFC 25 CURVE 加密认证；
  - [ ] IPC (`ipc://`) 本地命名管道与域套接字通信支持；
  - [ ] 零拷贝内存池与批量向量化发送优化。

---

## 开源许可证

本项目基于 [MIT 许可证](LICENSE) 开源。欢迎社区开发者提出 Issue 与贡献代码！
