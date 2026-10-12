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
		Topics:          make(map[string]*Topic),
		DataDir:         "data_test",
		DeliveryTimeout: 200 * time.Millisecond,
		BatchSize:       100,
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

	msgs, nextOffset, err := b.Consume("topicA", "default")
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

// TestConsumeEdgeCases 边界情况
func TestConsumeEdgeCases(t *testing.T) {
	b := newTestBroker()

	// 1. 不存在的 topic
	_, _, err := b.Consume("notExist", "default")
	if err == nil {
		t.Error("期望消费不存在的 topic 返回 error，实际 nil")
	}

	// 2. 空 topic，offset = 0
	b.Publish("empty", "only")
	_, _, err = b.Consume("empty", "default")
	if err != nil {
		t.Errorf("offset == NextOffset 时期望 nil error，实际 %v", err)
	}
}

// TestMultipleTopics 多 topic 隔离
func TestMultipleTopics(t *testing.T) {
	b := newTestBroker()

	b.Publish("A", "a1")
	b.Publish("A", "a2")
	b.Publish("B", "b1")

	msgsA, _, _ := b.Consume("A", "default")
	msgsB, _, _ := b.Consume("B", "default")

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
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

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

	expected := numGoroutines * msgsPerGoroutine

	// 循环 Consume，直到拿满 expected 条
	seen := make(map[int64]bool)
	for len(seen) < expected {
		msgs, _, err := b.Consume("concurrent", "default")
		if err != nil {
			t.Fatalf("Consume 错误: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatalf("已消费 %d 条，但还需 %d 条，Consume 返回空", len(seen), expected-len(seen))
		}
		for _, msg := range msgs {
			if seen[msg.ID] {
				t.Errorf("ID %d 重复", msg.ID)
			}
			seen[msg.ID] = true
		}
	}

	// 验证 ID 连续无重复
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
	msgs, next, err := b2.Consume("persist", "default")
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
	if err := b1.Ack("ackp", "default", 0); err != nil {
		t.Fatal(err)
	}
	if err := b1.Ack("ackp", "default", 1); err != nil {
		t.Fatal(err)
	}

	// 模拟重启：新建 Broker，loadFromDisk
	b2 := newTestBroker()
	if err := b2.loadFromDisk(); err != nil {
		t.Fatal(err)
	}

	// 应该只剩第 3 条（ID=2）未确认
	msgs, _, err := b2.Consume("ackp", "default")
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

	// 发 3 条
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

	// 消费者 1：拉全部，ACK 前两条，然后崩溃（断开连接）
	conn2, err := net.Dial("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("consumer dial 失败: %v", err)
	}
	enc2 := json.NewEncoder(conn2)
	dec2 := json.NewDecoder(conn2)

	enc2.Encode(Request{Cmd: CmdConsume, Topic: "workflow"})
	var resp Response
	dec2.Decode(&resp)

	if len(resp.Messages) != 3 {
		t.Fatalf("期望 3 条消息，实际 %d 条", len(resp.Messages))
	}

	for i := 0; i < 2; i++ {
		enc2.Encode(Request{Cmd: CmdAck, Topic: "workflow", Offset: int64(i)})
		var ackResp Response
		dec2.Decode(&ackResp)
		if ackResp.Status != StatusOK {
			t.Fatalf("ACK %d 失败: %+v", i, ackResp)
		}
	}

	// 模拟崩溃：直接关闭连接（第 3 条没 ACK，仍在 Pending 里）
	conn2.Close()

	// 等超时（newTestBroker 里 timeout 是 200ms）
	time.Sleep(300 * time.Millisecond)

	// 新消费者重启：应该只拿到第 3 条（ID=2）
	conn3, err := net.Dial("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("new consumer dial 失败: %v", err)
	}
	defer conn3.Close()

	enc3 := json.NewEncoder(conn3)
	dec3 := json.NewDecoder(conn3)

	enc3.Encode(Request{Cmd: CmdConsume, Topic: "workflow"})
	var resp3 Response
	dec3.Decode(&resp3)

	if len(resp3.Messages) != 1 {
		t.Fatalf("重启消费者后期望 1 条未确认消息，实际 %d 条", len(resp3.Messages))
	}
	if resp3.Messages[0].ID != 2 {
		t.Errorf("期望拿到第 3 条消息(ID=2)，实际 ID=%d", resp3.Messages[0].ID)
	}
}

func TestRedelivery(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b := newTestBroker()
	b.Publish("rd", "msg1")

	// 第一次消费
	msgs, _, _ := b.Consume("rd", "default")
	if len(msgs) != 1 {
		t.Fatalf("期望 1 条")
	}

	// 不 ACK，等超时
	time.Sleep(600 * time.Millisecond)

	// 后台清理虽然没跑，但 Consume 依然返回（Pending 里存在不影响）
	msgs, _, _ = b.Consume("rd", "default")
	if len(msgs) != 1 {
		t.Fatalf("超时后应重投，实际 %d 条", len(msgs))
	}
}

func TestGroupIsolation(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b := newTestBroker()

	// 发 3 条
	for i := 0; i < 3; i++ {
		if _, err := b.Publish("iso", "msg"); err != nil {
			t.Fatal(err)
		}
	}

	// 组 A 消费全部 3 条，ACK 全部
	msgsA, _, err := b.Consume("iso", "groupA")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsA) != 3 {
		t.Fatalf("组 A 期望 3 条，实际 %d 条", len(msgsA))
	}
	for i := 0; i < 3; i++ {
		if err := b.Ack("iso", "groupA", int64(i)); err != nil {
			t.Fatal(err)
		}
	}

	// 组 B 消费：应该拿到全部 3 条（不受组 A 影响）
	msgsB, _, err := b.Consume("iso", "groupB")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsB) != 3 {
		t.Fatalf("组 B 期望 3 条，实际 %d 条", len(msgsB))
	}

	// 组 A 再消费：已经 ACK 全部 3 条，应该拿到 0 条
	msgsA2, _, err := b.Consume("iso", "groupA")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsA2) != 0 {
		t.Errorf("组 A 已 ACK 全部，期望 0 条，实际 %d 条", len(msgsA2))
	}
}

func TestGroupNoDuplicate(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b := newTestBroker()

	// 发 6 条消息
	for i := 0; i < 6; i++ {
		if _, err := b.Publish("grp", "msg"); err != nil {
			t.Fatal(err)
		}
	}

	// 同一组（g1）两个消费者，顺序拉取
	msgsA, nextA, err := b.Consume("grp", "g1")
	if err != nil {
		t.Fatal(err)
	}

	msgsB, nextB, err := b.Consume("grp", "g1")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("消费者 A 拿到 %d 条，next=%d", len(msgsA), nextA)
	t.Logf("消费者 B 拿到 %d 条，next=%d", len(msgsB), nextB)

	// 核心断言：两个消费者拿到的消息集合不能有交集
	seen := make(map[int64]string)
	for _, m := range msgsA {
		if who, ok := seen[m.ID]; ok {
			t.Errorf("消息 %d 被 %s 和 A 同时拿到", m.ID, who)
		}
		seen[m.ID] = "A"
	}
	for _, m := range msgsB {
		if who, ok := seen[m.ID]; ok {
			t.Errorf("消息 %d 被 %s 和 B 同时拿到", m.ID, who)
		}
		seen[m.ID] = "B"
	}

	// 已知限制：当前没有 batch size，A 会一次性把所有消息拿走
	// 所以 B 拿到 0 条是"预期行为"，而不是 bug
	// 这个测试验证的是"不重复"，不是"分摊均匀"
	if len(msgsA)+len(msgsB) != 6 {
		t.Errorf("期望总共消费 6 条，实际 %d", len(msgsA)+len(msgsB))
	}
}

func TestGroupBatchDistribution(t *testing.T) {
	os.RemoveAll("data_test")
	os.MkdirAll("data_test", 0755)

	b := newTestBroker()

	// 发 6 条消息
	for i := 0; i < 6; i++ {
		if _, err := b.Publish("grp2", "msg"); err != nil {
			t.Fatal(err)
		}
	}

	// 手动把 BatchSize 改成 2（同包测试可以访问未导出字段）
	b.Mu.RLock()
	tp := b.Topics["grp2"]
	b.Mu.RUnlock()
	tp.Mu.Lock()
	tp.BatchSize = 2
	tp.Mu.Unlock()

	// 同一组 g1 的三个消费者，各拿一批
	msgsA, nextA, err := b.Consume("grp2", "g1")
	if err != nil {
		t.Fatal(err)
	}
	msgsB, nextB, err := b.Consume("grp2", "g1")
	if err != nil {
		t.Fatal(err)
	}
	msgsC, nextC, err := b.Consume("grp2", "g1")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("A: %d 条, next=%d", len(msgsA), nextA)
	t.Logf("B: %d 条, next=%d", len(msgsB), nextB)
	t.Logf("C: %d 条, next=%d", len(msgsC), nextC)

	if len(msgsA) != 2 || len(msgsB) != 2 || len(msgsC) != 2 {
		t.Fatalf("期望各拿 2 条，实际 %d/%d/%d", len(msgsA), len(msgsB), len(msgsC))
	}

	// 验证不重复
	seen := make(map[int64]string)
	all := []struct {
		who  string
		msgs []Message
	}{
		{"A", msgsA}, {"B", msgsB}, {"C", msgsC},
	}
	for _, item := range all {
		for _, m := range item.msgs {
			if who, ok := seen[m.ID]; ok {
				t.Errorf("消息 %d 被 %s 和 %s 重复拿到", m.ID, who, item.who)
			}
			seen[m.ID] = item.who
		}
	}

	// 验证覆盖全部 6 条
	if len(seen) != 6 {
		t.Errorf("期望覆盖 6 条消息，实际 %d 条", len(seen))
	}
	for i := int64(0); i < 6; i++ {
		if _, ok := seen[i]; !ok {
			t.Errorf("消息 %d 没有被任何消费者拿到", i)
		}
	}

	// 再拉一次应该拿不到（都分完了）
	msgsD, _, err := b.Consume("grp2", "g1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsD) != 0 {
		t.Errorf("消息已分完，期望 0 条，实际 %d 条", len(msgsD))
	}
}
