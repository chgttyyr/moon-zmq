# moon-zmq 架构设计说明

本文档详细阐述 `moon-zmq` 的系统架构、分层模型、异步 I/O 驱动机制以及目标平台支持策略。

---

## 一、系统架构与分层模型

`moon-zmq` 采用分层解耦的模块化设计，自底向上分为五层：

```
+-------------------------------------------------------------------+
|                           应用层 API                              |
|           Context / Socket / Message / Poller (可选扩展)          |
+-------------------------------------------------------------------+
|                        Socket 模式层 (RFC)                        |
|  REQ/REP (RFC 28) | DEALER/ROUTER (RFC 31) | PUB/SUB (RFC 29)     |
|  PUSH/PULL (RFC 30) | XPUB/XSUB (RFC 47) | PAIR                   |
+-------------------------------------------------------------------+
|                      传输与会话连接管理层                         |
|      TransportHub / Pipe / PipeWriter / Reconnector / HWM         |
+-------------------------------------------------------------------+
|                      握手协商与安全认证层                         |
|    Greeting / NULL (RFC 23) / PLAIN (RFC 24) / ZAP Authenticator  |
+-------------------------------------------------------------------+
|                       底层 ZMTP 协议核心层                        |
|           Frame / FrameDecoder / FrameEncoder / Command           |
+-------------------------------------------------------------------+
```

### 1. 协议核心层 (ZMTP 3.1 Core)
- **帧编解码器 (`FrameDecoder`, `FrameEncoder`)**：
  - 严格按照 RFC 23 解析与组装 ZMTP 3.1 帧格式，涵盖短数据帧（`< 255` 字节）、长数据帧（`8` 字节大端长度）、`MORE` 延续分帧标志位以及命令帧标志位。
  - 流式状态机设计：解码器支持跨网络数据包边界的流式输入（增量 `feed`），具备半包缓存、粘包自动切分及超过 `max_frame_size` 的防护机制。
- **协议命令对象 (`ZmtpCommand`)**：
  - 支持 `READY`、`ERROR`、`SUBSCRIBE`、`CANCEL`、`PING`、`PONG` 等标准命令的二进制序列化与反序列化。
  - 元数据编解码器 (`Metadata`)：负责处理键值对元数据（如 `Socket-Type`、`Identity`、`Resource` 等）的解析与格式校验。

### 2. 握手协商与安全机制层 (Handshake & Security)
- **问候帧协商 (`Greeting`)**：
  - 发起端与接收端对齐 64 字节的固定格式问候帧：`\xFF` 签名位、版本主次号（`3.1`）、安全机制名字符串（如 `"NULL"`、`"PLAIN"`）以及 `as-server` 标记位。
- **NULL 认证机制**：
  - 针对无认证场景，在双向完成 Greeting 握手后，立即互相发送并验证 `READY` 命令帧，交换各自套接字类型与 Identity。
- **PLAIN 认证机制 (RFC 24)**：
  - 实现了基于用户名与密码的明文认证交互流。
  - 客户端握手流程：发送 `HELLO` 命令帧（携带用户名和密码），等待服务端返回 `WELCOME`；接收到 `WELCOME` 后发送 `INITIATE` 命令帧交换客户端元数据，最终等待服务端返回 `READY`。
  - 服务端握手流程：接收 `HELLO` 帧，调用可配置的 ZAP（ZeroMQ Authentication Protocol）验证回调；校验通过后回送 `WELCOME`，接收 `INITIATE`，最后回复 `READY` 建立通信；认证失败则直接发送 `ERROR` 命令帧并安全切断连接。

### 3. 传输与会话连接管理层 (Transport & Connection Management)
- **多端点管理 (`TransportHub`)**：
  - 集中管理主动发起的对外连接（`connect`）以及本地端口监听并接受的入站连接（`bind`）。
  - 连接管道抽象（`Pipe`）：将底层的异步 TCP Socket 封装为具有独立收发队列的会话管道。
- **高水位标记 (HWM) 与反压控制**：
  - 为每个管道设置 `sndhwm` 与 `rcvhwm`。
  - 发送端队列满载时，根据套接字模式实行阻塞等待（反压）或基于策略丢弃，防止内存无上限膨胀。
- **自动重连与指数退避 (`Reconnector`)**：
  - 针对客户端连接，当底层连接断开或网络异常时，自动启动带有可配置初始间隔（`reconnect_ivl`）和最大指数退避（`reconnect_ivl_max`）的异步重连循环。

### 4. Socket 模式层 (Socket Pattern State Machines)
每个 Socket 模式严格遵循对应的 ZeroMQ RFC 行为准则：
- **REQ / REP (RFC 28)**：内置双向交替状态机（`SendReady` 与 `RecvReady`），禁止乱序发送或连续接收，自动封装和剥离包含空帧（Empty Delimiter Frame）的请求应答信封。
- **DEALER / ROUTER (RFC 31)**：
  - ROUTER 支持异步多路复用，自动在接收到的消息最前部附加来源连接的 `Routing-Id`，并在发送时根据首帧 `Routing-Id` 定向路由至目标 Peer；支持 `ZMQ_ROUTER_MANDATORY` 策略，未找到目标节点时抛出异常。
  - DEALER 支持全双工无状态交互，对所有已就绪的对端实行公平轮询（Round-Robin）分发与公平接收（Fair-Queuing）。
- **PUB / SUB (RFC 29) & XPUB / XSUB (RFC 47)**：
  - PUB 维护订阅主题字典树或前缀列表，对所有已匹配的订阅者广播推送数据；
  - SUB 在建立连接后自动向上游发送 `\x01` 前缀的 `SUBSCRIBE` 命令或数据帧，接收时由本地过滤器二次校验；
  - XPUB / XSUB 支持将下游订阅变动向上游透传，满足构建分级发布订阅代理的需求。
- **PUSH / PULL (RFC 30)**：
  - PUSH 实现基于轮询（Round-Robin）的单向任务分发；
  - PULL 实现基于公平排队（Fair-Queuing）的单向任务汇聚。
- **PAIR**：
  - 点对点独占双向通信通道。

### 5. 公开接口层 (Context & Socket API)
- 提供符合现代 ZeroMQ 使用习惯的上下文（`Context`）生命周期管理。
- 线程安全与协程安全的消息载荷结构（`Message`），提供单帧及多帧消息（Multipart Message）的统一抽象。
- **RFC 43 可控代理（`proxy`, `proxy_steerable`）**：实现全自动双向流量中继与可选抓包（Capture），支持通过 control 套接字实现运行时的 PAUSE / RESUME / TERMINATE 动态管控与流量统计（`ProxyStats`）。
- **统一多路复用器（`Poller`）**：支持在单个异步协程中统一非阻塞轮询或超时等待多个不同类型的套接字，精准捕获 `POLLIN`、`POLLOUT`、`POLLERR` 事件。
- **ZMTP 3.1 心跳保活与活性超时检测（`set_heartbeat`）**：支持按指定间隔发送 `PING` 命令帧，自动应答 `PONG`，并在静默超时后安全熔断连接并触发自动重连。
- **套接字生命周期事件监听（`SocketEvent`）**：提供细粒度的事件捕获与生命周期监控（`Listening`、`Accepted`、`Connected`、`Disconnected`、`HandshakeSucceeded`、`HandshakeFailed`、`Closed`），全面支持链路健康探针与分布式可观测性诊断。
- **实时流量指标与批量 I/O（`send_batch` / `recv_batch`）**：套接字内置收发消息数、字节数以及活跃 Peer 列表的零成本统计；提供批量消息处理接口，在高吞吐并发场景下大幅减少跨协程上下文切换。
- **双传输层统一抽象（`tcp://` & `inproc://`）**：除跨进程原生 TCP 外，提供 `Context` 级隔离的 `inproc://` 零内核拷贝全双工内存通道，原生支持先 `connect` 后 `bind` 的延迟绑定唤醒机制。
- **高可靠分布式模式与 CZMQ 并发引擎**：内置 RFC 7 Majordomo (`MDP/0.2`) 服务代理与 RFC 8 `MMI` 服务发现、`ReliableClient` 多端点故障转移、`Radio`/`Dish` 精确群组发布订阅、`SubscriptionTrie` / `LastValueCache` / `ConflateBuffer`、RFC 4 `ZConfig` 配置树、`ZActor` / `ZLoop` 反应器，以及 `WireDissector`、微秒级 `LatencyHistogram`、双令牌桶 `RateLimiter` 与三态 `CircuitBreaker` 熔断器。

---

## 二、异步 I/O 驱动方案

`moon-zmq` 采用 MoonBit 官方原生的协作式协程库 `moonbitlang/async` 构建全异步无阻塞网络引擎：

1. **事件驱动底座**：
   - 在 native 目标平台下，`moonbitlang/async` 依托操作系统原生的事件通知机制（Windows 平台采用 IOCP / 完成端口，Unix/Linux 平台采用 epoll / kqueue），实现百万级并发连接的高吞吐 I/O 调度。
2. **结构化并发 (`TaskGroup`)**：
   - 所有的后台监听器（Acceptor Loop）、连接守护任务（Session Reader/Writer Loop）以及自动重连任务均托管于结构化并发任务组内。
   - 当调用 `Socket.close()` 或 `Context.term()` 时，父级任务组的取消信号级联传播至所有下属连接，确保底层文件描述符与内存资源能够被确定性、无泄漏地回收。
3. **零锁化与通道通信**：
   - 管道收发借助异步队列与事件通知实现无锁化/极低竞争的读写协同，杜绝由于系统级线程锁争用引发的高并发性能衰减。

---

## 三、编译目标限定说明

`moon-zmq` 当前版本**明确限定支持 Native 编译目标**（`--target native`，涵盖 x86_64、AArch64 架构的 Linux、macOS 与 Windows 操作系统）。

### 目标选择考量：
1. **原生网络能力**：
   - ZeroMQ 属于系统级高性能消息总线，核心场景在于跨进程、跨微服务、分布式计算节点及异构语言系统间的底层 TCP 套接字通信。Native 目标直接编译为机器码，能够直接调用操作系统的 BSD Socket 和高效异步 I/O 子系统。
2. **WebAssembly / JavaScript 平台的限制**：
   - 浏览器端 WebAssembly 以及标准 JavaScript 运行时受到沙箱安全限制，不支持开放原始 TCP 监听（`listen`/`bind`）或任意 TCP 全双工直连能力。
   - 在 Wasm/JS 环境中运行 ZeroMQ 通常需要额外的 WebSocket 桥接网关或特制的代理服务。因此，将核心资源聚焦于工业生产与高性能计算所需的 Native 目标，是确保协议完整度与运行效率的最佳技术路线。
