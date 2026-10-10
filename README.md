# MyTinyMQ

一个基于 Go 的极简消息队列，支持内存队列、TCP 网络、消息持久化、ACK 确认、超时重投和消费者组。

## 功能

- **发布/消费**：`Publish` 往 topic 追加消息，`Consume` 按 offset 拉取消息
- **TCP 网络层**：一行一个 JSON 的协议，支持多客户端并发连接
- **消息持久化**：每条消息追加写到 `.log` 文件，重启后从磁盘恢复
- **ACK 确认**：消费者处理完消息后发送 ACK，Broker 记录消费进度，重启后不重复投递
- **超时重投**：已投递但未 ACK 的消息，超过 `DeliveryTimeout` 后重新变为可消费
- **消费者组**：不同组之间进度独立，互不影响
- **并发安全**：Broker 和 Topic 各自加锁，`go test -race` 全部通过

## 协议

基于 TCP，一行一个 JSON，`\n` 结尾。

**请求：**

```json
{"cmd": "publish", "topic": "A", "body": "hello"}
{"cmd": "consume", "topic": "A", "offset": 0, "group": "groupA"}
{"cmd": "ack",     "topic": "A", "offset": 0, "group": "groupA"}
```

```group``` 字段可选，不填时默认使用 ```"default"``` 组

**响应：**

```json
{"status": "ok", "offset": 0}
{"status": "ok", "messages": [{"id": 0, "topic": "A", "body": "hello"}], "nextOffset": 1}
{"status": "error", "error": "topic not found"}
```

## 目录结构

```text
mini-mq/
├── main.go            # 启动入口
├── broker.go          # Broker：管理所有 Topic，对外提供 Publish/Consume/Ack
├── topic.go           # Topic：消息列表、offset、ACK 进度
├── message.go         # Message：一条消息的数据结构
├── protocol.go        # 网络协议：命令枚举、请求/响应结构
├── broker_test.go     # 单元测试 + 集成测试
└── data/              # 持久化目录
    ├── <topic>.log    # 消息日志，一行一个 JSON
    └── <topic>.meta   # 消费进度，{"ackedOffset": N}
```

## 运行

```bash
go run .
```
Broker 默认监听 :9092。

## 测试

```bash
go test -race -v ./...
```
测试覆盖：

- 基本发布和消费
- 分段消费（从指定 offset 拉取）
- 边界情况（不存在的 topic、非法 offset）
- 多 topic 隔离
- 并发发布（10 goroutine × 100 条）
- 消息持久化（重启后恢复）
- ACK 持久化（重启后不重复投递）
- 网络层集成测试（真实 TCP 连接，模拟消费者 ACK + 崩溃）
- 超时重投（未 ACK 的消息超时后可重新消费）
- 消费者组隔离（组 A 的 ACK 不影响组 B）

## 设计说明

### 为什么消息和消费进度分开存

消息是不可变的历史数据，消费进度是持续更新的状态。两者生命周期不同，存在同一文件会让日志追加和进度覆盖互相干扰。所以拆成 .log 和 .meta 两个文件：

- ```.log```：只追加，不修改
- ```.meta```：每次 ACK 覆盖写

这和 Kafka 的 ```__consumer_offsets``` 设计思路一致。

### 为什么 ACK 后不删消息

删除是破坏性的，消息一旦删掉就无法重放、无法排查。保留消息 + 记录 AckedOffset 是主流做法，代价是需要额外的清理机制（本项目暂未实现）。

### 一致性保证

写入顺序是：先写磁盘，成功后再改内存。这样内存和磁盘始终一致，不会出现“内存有、磁盘没有”的脏状态。

### 超时重投

```GroupState``` 维护一个 ```Pending map[int64]time.Time```，记录该组已投递但未 ACK 的消息及其投递时间。

- ```Consume``` 时，把返回的消息记入 ```Pending```；
- ```Ack``` 时，从 ```Pending``` 中删除；

后台 goroutine 每秒扫描一次，超过 ```DeliveryTimeout```（默认 5 秒）的条目从 ```Pending``` 中移除，让它们重新变为可消费。

这实现了至少一次（at-least-once）语义：消息可能被重复投递，但不会丢。

### 消费者组

每个组有独立的 ```GroupState```：
```go
type GroupState struct {
    AckedOffset    int64
    NextReadOffset int64
    Pending        map[int64]time.Time
}
```

- 不同组之间：```AckedOffset``` 和 ```Pending``` 完全独立，组 A 的消费进度不影响组 B；
- 同一个组内：共享 ```AckedOffset``` 和 ```Pending```（组内多消费者分发暂未实现）；
- 组名通过协议的 ```group``` 字段传递，不填默认 ```"default"```。

```NextReadOffset``` 字段已预留，用于后续实现组内多消费者分摊消息。

## 已知限制

- 组内多消费者分发（```NextReadOffset```）未实现，同一组内多个消费者会重复消费
- 未 ACK 的消息超时后重新投递，但消费者无法主动拒绝（无 NACK）
- 消息文件不清理，长期运行会一直增长
- 未实现认证、限流、压缩
- ```DeliveryTimeout``` 硬编码在 Topic 中，未做成可配置

## 后续可扩展方向

- 组内多消费者分发：用 NextReadOffset 让同一组内的消费者各拿一段
- 批量 ACK：减少网络往返
- NACK + 死信队列：消费者主动拒绝的消息进入死信
- 消息文件分段 + 清理策略
- 可配置的 DeliveryTimeout

# License
仅用于个人学习。