package main

import (
	"encoding/json"
	"net"
	"testing"
)

func TestNetworkPublishConsume(t *testing.T) {
	b := &Broker{Topics: make(map[string]*Topic)}

	// 1. 起临时 listener
	listener, err := net.Listen("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("listen 失败: %v", err)
	}
	defer listener.Close()

	// 2. 后台 accept
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go b.handleConn(conn)
		}
	}()

	// 3. 客户端连上
	conn, err := net.Dial("tcp", "127.0.0.1:19092")
	if err != nil {
		t.Fatalf("dial 失败: %v", err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// 4. publish
	encoder.Encode(Request{Cmd: CmdPublish, Topic: "A", Body: "hello"})
	var resp Response
	decoder.Decode(&resp)
	if resp.Status != StatusOK {
		t.Fatalf("publish 失败: %+v", resp)
	}
	if resp.Offset != 0 {
		t.Errorf("期望 offset = 0，实际 %d", resp.Offset)
	}

	// 5. consume
	encoder.Encode(Request{Cmd: CmdConsume, Topic: "A", Offset: 0})
	decoder.Decode(&resp)
	if len(resp.Messages) != 1 {
		t.Fatalf("期望 1 条消息，实际 %d 条", len(resp.Messages))
	}
	if resp.Messages[0].Body != "hello" {
		t.Errorf("期望 body = hello，实际 %s", resp.Messages[0].Body)
	}
}
