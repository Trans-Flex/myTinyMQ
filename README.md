# MyTinyMQ

一个基于 Go 的极简消息队列，支持内存队列、TCP 网络、消息持久化、ACK 确认、超时重投和消费者组。

## 功能

- **发布/消费**：`Publish` 往 topic 追加消息，`Consume` 按消费者组拉取消息
- **TCP 网络层**：一行一个 JSON 的协议，支持多客户端并发连接
- **消息持久化**：每条消息追加写到 `.log` 文件，重启后从磁盘恢复
- **ACK 确认**：消费者处理完消息后发送 ACK，Broker 记录消费进度，重启后不重复投递
- **超时重投**：已投递但未 ACK 的消息，超过 `DeliveryTimeout` 后重新变为可消费
- **消费者组**：不同组之间进度独立；同一组内多个消费者按 `BatchSize` 分摊消息
- **并发安全**：Broker 和 Topic 各自加锁，`go test -race` 全部通过

## 协议

基于 TCP，一行一个 JSON，`\n` 结尾。

**请求：**

```json
{"cmd": "publish", "topic": "A", "body": "hello"}
{"cmd": "consume", "topic": "A", "group": "groupA"}
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
- 边界情况（不存在的 topic）
- 多 topic 隔离
- 并发发布（10 goroutine × 100 条）
- 消息持久化（重启后恢复）
- ACK 持久化（重启后不重复投递）
- 网络层集成测试（真实 TCP 连接，模拟消费者 ACK + 崩溃）
- 超时重投（未 ACK 的消息超时后可重新消费）
- 消费者组隔离（组 A 的 ACK 不影响组 B）
- 组内不重复（同一组两个消费者拿到不重叠的消息）
- 组内分摊（BatchSize 限制下，多个消费者各拿一批）

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

### 消费者组模型

```GroupState``` 结构：

```go
type GroupState struct {
    AckedOffset int64
    Pending     map[int64]time.Time
}
```

- ```AckedOffset```：这个组已确认到哪；
- ```Pending```：这个组已分发但未 ACK 的消息，key 是消息 ID，value 是投递时间。

```Consume``` 的分发逻辑：

1. 从 ```AckedOffset``` 开始扫描消息；
2. 跳过 ```Pending``` 里未超时的（说明已有人领了，正在处理）；
3. 收满 ```BatchSize``` 条就停；
4. 把收进结果的消息记入 ```Pending```。

这实现了组内分摊：消费者 A 领了前 100 条，B 来时 A 还没 ACK，那 100 条在 Pending 里被跳过，B 自动拿到下一批。

不同组之间完全独立：每个组有自己的 AckedOffset 和 Pending，互不影响。

### 超时重投

后台 goroutine 每秒扫描一次所有组的 ```Pending```，超过 ```DeliveryTimeout``` 的条目从 ```Pending``` 中删除，让它们重新变为可消费。

这实现了至少一次（at-least-once）语义：消息可能被重复投递，但不会丢。

### 为什么 ```Consume``` 不接收 offset 参数

在这个模型下，消费位置由 ```AckedOffset``` 和 ```Pending``` 共同决定，消费者不需要也不应该指定从哪读。这是"Broker 分发"模型，而不是 Kafka 那种"消费者自己维护 offset"的模型。

Kafka 有分区概念，一个分区同一时间只有一个消费者，所以消费者自己维护 offset 是安全的。我们没有分区，如果消费者自己传 offset，多个消费者会抢同一段，所以选择 Broker 分发。

### 可配置参数

```Broker``` 上有两个可配置字段：

- ```DeliveryTimeout```：超时重投的阈值，默认 5 秒；
- ```BatchSize```：每次 ```Consume``` 最多返回多少条，默认 100。

测试里通过 ```newTestBroker``` 设短超时（200ms），避免测试跑 5 秒。

### 配置归属

```BatchSize``` 和 ```DeliveryTimeout```都是 Broker 级配置，因为它们是"消费行为"的参数，不属于某个 topic：
- ```DeliveryTimeout```：超时重投的阈值，默认 5 秒；
- ```BatchSize```：每次 ```Consume``` 最多返回多少条，默认 100。

```Topic``` 不再持有这两个字段。创建 ```Topic```时也不再复制它们，避免"同一值存两处"的隐患。

### 构造函数

- ```NewBroker(dataDir, deliveryTimeout, batchSize)```：初始化 Broker；
- ```NewTopic(name, dataDir)```：初始化 Topic 的公共字段（路径、GroupStates 等）。

```Publish``` 和 ```loadFromDisk```都通过 ```NewTopic``` 创建 Topic，避免字段初始化在多个地方各写一遍。

注意：```loadFromDisk``` 恢复消息后，需要手动设置 ```t.Messages = msgs``` 和 ```t.NextOffset = int64(len(msgs))```，因为构造函数不接收这两个字段

## 已知限制

- ACK 之后不删消息，消息文件不清理，长期运行会一直增长；
- 不支持消费者主动拒绝（NACK），只能靠超时重投；
- 不支持 seek / 重放历史消息，消费者只能从 ```AckedOffset``` 往后读；
- ```Pending``` 里的消息只能靠超时清理，消费者 ACK 后才能从 ```Pending``` 移除；
- 单机版，没有分布式协调，```Broker``` 重启后 ```Pending``` 会丢失（未 ACK 的消息会重新可消费，但已投递未 ACK 的"投递状态"不保留）；
- 未实现认证、限流、压缩；
- 未实现批量 ACK，每条消息单独发一次 ACK；
- 没有分区，一个 topic 的消费顺序由单机保证。

## 后续可扩展方向

- 批量 ACK：一次 ACK 多个 offset，减少网络往返；
- NACK + 死信队列：消费者主动拒绝的消息进入死信；
- 消息文件分段 + 清理策略：按大小或时间分段，删除旧段；
- 消息压缩：对 ```.log``` 文件做压缩；
- 分区支持：一个 topic 分多个 partition，每个 partition 独立消费；
- 分布式部署：多机 Broker + 一致性协议（Raft）。

# License
仅用于个人学习。