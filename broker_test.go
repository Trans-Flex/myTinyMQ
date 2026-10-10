package main

import (
	"encoding/json"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestBroker() *Broker {
	return &Broker{
		Topics:  make(map[string]*Topic),
		DataDir: "data_test",
	}
}

// TestPublishAndConsume 基本发布和消费
func TestPublishAndConsume(t *testing.T) {
	b := newTestBroker()

	_, err := b.Publish("topicA", "msg1")
	if err != nil {
		t.Fatalf("Publish 失败: %v", err)
	}
	_, err = b.Publish("topicA", "msg2")
	if err != nil {
		t.Fatalf("Publish 失败: %v", err)
	}
	b.Publish("topicA", "msg3")
	if err != nil {
		t.Fatalf("Publish 失败: %v", err)
	}

	msgs, nextOffset, err := b.Consume("topicA", 0)
	if err != nil {
		t.Fatalf("Consume 返回错误: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("期望 3 条消息，实际 %d 条", len(msgs))
	}
	if nextOffset != 3 {
		t.Fatalf("期望 nextOffset = 3，实际 %d", nextOffset)
	}

	for i, msg := range msgs {
		expectedBody := "msg" + string(rune('1'+i))
		if msg.Body != expectedBody {
			t.Errorf("第 %d 条消息期望 %s，实际 %s", i, expectedBody, msg.Body)
		}
		if msg.ID != int64(i) {
			t.Errorf("第 %d 条消息期望 ID = %d，实际 %d", i, i, msg.ID)
		}
	}
}

// TestConsumeFromOffset 从指定 offset 消费
func TestConsumeFromOffset(t *testing.T) {
	b := newTestBroker()

	for i := 0; i < 5; i++ {
		b.Publish("topicB", "msg")
	}

	// 从 0 开始，拿到全部 5 条
	msgs, next, err := b.Consume("topicB", 0)
	if err != nil {
		t.Fatalf("Consume 错误: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("期望 5 条，实际 %d 条", len(msgs))
	}
	if next != 5 {
		t.Fatalf("期望 nextOffset = 5，实际 %d", next)
	}

	// 从 3 开始，拿到 2 条
	msgs, next, err = b.Consume("topicB", 3)
	if err != nil {
		t.Fatalf("Consume 错误: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("期望 2 条，实际 %d 条", len(msgs))
	}
	if msgs[0].ID != 3 {
		t.Errorf("期望第一条 ID = 3，实际 %d", msgs[0].ID)
	}
	if next != 5 {
		t.Fatalf("期望 nextOffset = 5，实际 %d", next)
	}
}

// TestConsumeEdgeCases 边界情况
func TestConsumeEdgeCases(t *testing.T) {
	b := newTestBroker()

	// 1. 不存在的 topic
	_, _, err := b.Consume("notExist", 0)
	if err == nil {
		t.Error("期望消费不存在的 topic 返回 error，实际 nil")
	}

	// 2. 空 topic，offset = 0
	b.Publish("empty", "only")
	_, _, err = b.Consume("empty", 1)
	if err != nil {
		t.Errorf("offset == NextOffset 时期望 nil error，实际 %v", err)
	}

	// 3. offset < 0
	_, _, err = b.Consume("empty", -1)
	if err == nil {
		t.Error("期望 offset < 0 返回 error，实际 nil")
	}

	// 4. offset > NextOffset
	_, _, err = b.Consume("empty", 100)
	if err == nil {
		t.Error("期望 offset > NextOffset 返回 error，实际 nil")
	}
}

// TestMultipleTopics 多 topic 隔离
func TestMultipleTopics(t *testing.T) {
	b := newTestBroker()

	b.Publish("A", "a1")
	b.Publish("A", "a2")
	b.Publish("B", "b1")

	msgsA, _, _ := b.Consume("A", 0)
	msgsB, _, _ := b.Consume("B", 0)

	if len(msgsA) != 2 {
		t.Errorf("topic A 期望 2 条，实际 %d 条", len(msgsA))
	}
	if len(msgsB) != 1 {
		t.Errorf("topic B 期望 1 条，实际 %d 条", len(msgsB))
	}
	if msgsA[0].Body != "a1" {
		t.Errorf("topic A 第一条期望 a1，实际 %s", msgsA[0].Body)
	}
	if msgsB[0].Body != "b1" {
		t.Errorf("topic B 第一条期望 b1，实际 %s", msgsB[0].Body)
	}
}

// TestConcurrentPublish 并发发布
func TestConcurrentPublish(t *testing.T) {
	b := newTestBroker()

	var wg sync.WaitGroup
	numGoroutines := 10
	msgsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < msgsPerGoroutine; j++ {
				b.Publish("concurrent", "msg")
			}
		}()
	}
	wg.Wait()

	msgs, next, err := b.Consume("concurrent", 0)
	if err != nil {
		t.Fatalf("Consume 错误: %v", err)
	}

	expected := numGoroutines * msgsPerGoroutine
	if len(msgs) != expected {
		t.Fatalf("期望 %d 条消息，实际 %d 条", expected, len(msgs))
	}
	if next != int64(expected) {
		t.Fatalf("期望 nextOffset = %d，实际 %d", expected, next)
	}

	// 验证 ID 连续无重复
	seen := make(map[int64]bool)
	for _, msg := range msgs {
		if seen[msg.ID] {
			t.Errorf("ID %d 重复", msg.ID)
		}
		seen[msg.ID] = true
	}
	for i := 0; i < expected; i++ {
		if !seen[int64(i)] {
			t.Errorf("缺少 ID %d", i)
		}
	}
}

func TestPersistence(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b1 := newTestBroker()
	// 注意：loadFromDisk 写死了 "data"，要改成可配置，或者测试直接用 "data"
	// 简单起见，测试里用默认 data 目录，测试后清理

	// 发 3 条
	for i := 0; i < 3; i++ {
		_, err := b1.Publish("persist", "msg")
		if err != nil {
			t.Fatal(err)
		}
	}

	// 模拟重启：新建 Broker，loadFromDisk
	b2 := newTestBroker()
	if err := b2.loadFromDisk(); err != nil {
		t.Fatal(err)
	}

	// 验证数据还在
	msgs, next, err := b2.Consume("persist", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("期望 3 条，实际 %d", len(msgs))
	}
	if next != 3 {
		t.Fatalf("期望 nextOffset = 3，实际 %d", next)
	}
}

func TestAckPersistence(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	// 第一个 Broker：发 3 条，ACK 前两条
	b1 := newTestBroker()
	for i := 0; i < 3; i++ {
		if _, err := b1.Publish("ackp", "msg"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b1.Ack("ackp", 0); err != nil {
		t.Fatal(err)
	}
	if err := b1.Ack("ackp", 1); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：新建 Broker，loadFromDisk
	b2 := newTestBroker()
	if err := b2.loadFromDisk(); err != nil {
		t.Fatal(err)
	}

	// 应该只剩第 3 条（ID=2）未确认
	msgs, _, err := b2.Consume("ackp", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("重启后期望 1 条未确认消息，实际 %d", len(msgs))
	}
	if msgs[0].ID != 2 {
		t.Errorf("期望 ID = 2，实际 %d", msgs[0].ID)
	}
}

func TestNetworkConsumerWithAck(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b := newTestBroker()

	// 1. 启动测试 Broker，监听 :19092
	listener, err := net.Listen("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("listen 失败: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go b.handleConn(conn)
		}
	}()

	// 2. 连接并发送 3 条消息
	conn1, err := net.Dial("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("dial 失败: %v", err)
	}
	defer conn1.Close()

	enc1 := json.NewEncoder(conn1)
	dec1 := json.NewDecoder(conn1)

	for i := 0; i < 3; i++ {
		enc1.Encode(Request{Cmd: CmdPublish, Topic: "workflow", Body: "task"})
		var resp Response
		dec1.Decode(&resp)
		if resp.Status != StatusOK {
			t.Fatalf("publish 失败: %+v", resp)
		}
	}

	// 3. 启动消费者，拉取消息
	conn2, err := net.Dial("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("consumer dial 失败: %v", err)
	}
	enc2 := json.NewEncoder(conn2)
	dec2 := json.NewDecoder(conn2)

	enc2.Encode(Request{Cmd: CmdConsume, Topic: "workflow", Offset: 0})
	var resp Response
	dec2.Decode(&resp)

	if len(resp.Messages) != 3 {
		t.Fatalf("期望 3 条消息，实际 %d 条", len(resp.Messages))
	}

	// 4. 模拟处理消息，然后 ACK 前两条
	for i := 0; i < 2; i++ {
		time.Sleep(10 * time.Millisecond) // 模拟处理
		enc2.Encode(Request{Cmd: CmdAck, Topic: "workflow", Offset: int64(i)})
		var ackResp Response
		dec2.Decode(&ackResp)
		if ackResp.Status != StatusOK {
			t.Fatalf("ACK %d 失败: %+v", i, ackResp)
		}
	}

	// 5. 模拟消费者“崩溃”，断开连接
	conn2.Close()

	// 6. 启动新的消费者，再次拉取，应该只剩第 3 条
	conn3, err := net.Dial("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("new consumer dial 失败: %v", err)
	}
	defer conn3.Close()

	enc3 := json.NewEncoder(conn3)
	dec3 := json.NewDecoder(conn3)

	enc3.Encode(Request{Cmd: CmdConsume, Topic: "workflow", Offset: 0})
	var resp3 Response
	dec3.Decode(&resp3)

	if len(resp3.Messages) != 1 {
		t.Fatalf("重启消费者后期望 1 条未确认消息，实际 %d 条", len(resp3.Messages))
	}
	if resp3.Messages[0].Body != "task" || resp3.Messages[0].ID != 2 {
		t.Errorf("期望拿到第 3 条消息(ID=2)，实际 ID=%d", resp3.Messages[0].ID)
	}
}

func TestRedelivery(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b := newTestBroker()
	b.Publish("rd", "msg1")

	// 第一次消费
	msgs, _, _ := b.Consume("rd", 0)
	if len(msgs) != 1 {
		t.Fatalf("期望 1 条")
	}

	// 不 ACK，等超时
	time.Sleep(600 * time.Millisecond)

	// 后台清理虽然没跑，但 Consume 依然返回（Pending 里存在不影响）
	msgs, _, _ = b.Consume("rd", 0)
	if len(msgs) != 1 {
		t.Fatalf("超时后应重投，实际 %d 条", len(msgs))
	}
}
