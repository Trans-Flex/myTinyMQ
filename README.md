# MyTinyMQ

一个基于 Go 的极简消息队列，支持内存队列、TCP 网络通信、文件持久化。

## 协议格式
采用一行一个 JSON 的通信协议（基于 TCP）：
- 请求：`{"cmd":"publish", "topic":"A", "body":"hello"}`
- 响应：`{"status":"ok", "offset":0}`

## 目录结构
- `broker.go`: 核心管理逻辑
- `topic.go`: Topic 定义
- `message.go`: 消息定义
- `protocol.go`: 网络协议定义
- `broker_test.go`: 单元与集成测试
- `data/`: 持久化日志目录

## 如何运行与测试
```bash
go test -race -v ./...