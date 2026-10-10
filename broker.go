package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	Topics  map[string]*Topic
	Mu      sync.RWMutex
	DataDir string
}

func (b *Broker) Publish(topic string, body string) (Message, error) {
	b.Mu.RLock()
	t, ok := b.Topics[topic]
	b.Mu.RUnlock()
	if !ok {
		b.Mu.Lock()
		t, ok = b.Topics[topic]
		if !ok {
			t = &Topic{
				Name:            topic,
				NextOffset:      0,
				FilePath:        filepath.Join(b.DataDir, topic+".log"),
				MetaPath:        filepath.Join(b.DataDir, topic+".meta"),
				GroupStates:     make(map[string]*GroupState),
				DeliveryTimeout: 5 * time.Second,
			}
			b.Topics[topic] = t
		}
		b.Mu.Unlock()
	}

	t.Mu.Lock()
	defer t.Mu.Unlock()
	msg := Message{
		ID:        t.NextOffset,
		Topic:     topic,
		Body:      body,
		Timestamp: time.Now(),
	}

	f, err := os.OpenFile(t.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return Message{}, err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(msg)
	if err != nil {
		return Message{}, err
	}

	t.Messages = append(t.Messages, msg)
	t.NextOffset++

	return msg, nil
}

func (b *Broker) Consume(topic, group string, offset int64) ([]Message, int64, error) {
	b.Mu.RLock()
	t, ok := b.Topics[topic]
	b.Mu.RUnlock()
	if !ok {
		return []Message{}, offset, fmt.Errorf("不存在的topic")
	}

	t.Mu.Lock()
	defer t.Mu.Unlock()

	g, ok := t.GroupStates[group]
	if !ok {
		g = &GroupState{
			AckedOffset:    0,
			NextReadOffset: 0,
			Pending:        make(map[int64]time.Time),
		}
		t.GroupStates[group] = g
	}

	if offset < 0 {
		return []Message{}, offset, fmt.Errorf("不合法的offset: %d", offset)
	}
	if offset > t.NextOffset {
		return []Message{}, offset, fmt.Errorf("超过NextOffset的offset: %d", offset)
	}
	if offset == t.NextOffset {
		return []Message{}, offset, nil
	}

	start := max(offset, g.AckedOffset)
	if start >= t.NextOffset {
		return []Message{}, t.NextOffset, nil
	}

	now := time.Now()
	var result []Message

	for i := start; i < t.NextOffset; i++ {
		msg := t.Messages[i]
		result = append(result, msg)
		g.Pending[msg.ID] = now // 只记录，不跳过
	}

	return result, t.NextOffset, nil
}

func (b *Broker) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go b.handleConn(conn)
	}
}

func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("read error: %v", err)
			}
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var req Request
		err = json.Unmarshal([]byte(line), &req)
		if err != nil {
			json.NewEncoder(conn).Encode(errorResponse("invalid json: " + err.Error()))
			continue
		}

		var resp Response
		switch req.Cmd {
		case CmdPublish:
			resp = b.handlePublish(req)
		case CmdConsume:
			resp = b.handleConsume(req)
		case CmdAck:
			resp = b.handleAck(req)
		default:
			resp = errorResponse("unknown cmd")
		}
		json.NewEncoder(conn).Encode(resp)
	}
}

func (b *Broker) handlePublish(req Request) Response {
	msg, err := b.Publish(req.Topic, req.Body)
	if err != nil {
		return Response{Status: StatusError, Error: err.Error()}
	}
	return Response{Status: StatusOK, Offset: msg.ID}
}

func (b *Broker) handleConsume(req Request) Response {
	group := groupOrDefault(req)
	msgs, next, err := b.Consume(req.Topic, group, req.Offset)
	if err != nil {
		return Response{Status: StatusError, Error: err.Error()}
	}
	return Response{Status: StatusOK, Messages: msgs, NextOffset: next}
}

func (b *Broker) handleAck(req Request) Response {
	group := groupOrDefault(req)
	if err := b.Ack(req.Topic, group, req.Offset); err != nil {
		return Response{Status: StatusError, Error: err.Error()}
	}
	return Response{Status: StatusOK}
}

func (b *Broker) loadFromDisk() error {
	err := os.MkdirAll(b.DataDir, 0755)
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(b.DataDir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}

		topic := strings.TrimSuffix(name, ".log")
		path := filepath.Join(b.DataDir, name)
		func() {
			f, err := os.Open(path)
			if err != nil {
				log.Printf("打开 %s 失败: %v", path, err)
				return
			}
			defer f.Close()

			var msgs []Message
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				var msg Message
				if err := json.Unmarshal([]byte(line), &msg); err != nil {
					log.Printf("跳过损坏的行: %v", err)
					continue
				}
				msgs = append(msgs, msg)
			}
			if err := scanner.Err(); err != nil {
				log.Printf("读取 %s 失败: %v", path, err)
				return
			}

			metaPath := filepath.Join(b.DataDir, topic+".meta")
			t := &Topic{Name: topic,
				Messages:        msgs,
				NextOffset:      int64(len(msgs)),
				FilePath:        path,
				MetaPath:        metaPath,
				GroupStates:     make(map[string]*GroupState),
				DeliveryTimeout: 5 * time.Second,
			}
			b.Topics[topic] = t

			if data, err := os.ReadFile(metaPath); err == nil {
				var meta struct {
					Groups map[string]int64 `json:"groups"`
				}
				if err := json.Unmarshal(data, &meta); err == nil {
					for name, acked := range meta.Groups {
						t.GroupStates[name] = &GroupState{
							AckedOffset:    acked,
							NextReadOffset: acked,
							Pending:        make(map[int64]time.Time),
						}
					}

				}
			}
			if len(t.GroupStates) == 0 {
				t.GroupStates["default"] = &GroupState{
					AckedOffset:    0,
					NextReadOffset: 0,
					Pending:        make(map[int64]time.Time),
				}
			}
		}()
	}

	return nil
}

func (b *Broker) Ack(topic, group string, offset int64) error {
	b.Mu.RLock()
	t, ok := b.Topics[topic]
	b.Mu.RUnlock()
	if !ok {
		return fmt.Errorf("不存在的topic: %s", topic)
	}

	t.Mu.Lock()
	defer t.Mu.Unlock()

	g, ok := t.GroupStates[group]
	if !ok {
		g = &GroupState{
			AckedOffset:    0,
			NextReadOffset: 0,
			Pending:        make(map[int64]time.Time),
		}
		t.GroupStates[group] = g
	}

	if offset < 0 {
		return fmt.Errorf("无效的offset: %d", offset)
	}
	if offset >= t.NextOffset {
		return fmt.Errorf("过大的offset: %d", offset)
	}
	if offset < g.AckedOffset {
		return nil
	}

	newOffset := offset + 1
	groups := make(map[string]int64)
	for name, gs := range t.GroupStates {
		groups[name] = gs.AckedOffset
	}
	groups[group] = newOffset

	data, _ := json.Marshal(map[string]any{"groups": groups})
	if err := os.WriteFile(t.MetaPath, data, 0644); err != nil {
		return err
	}
	g.AckedOffset = newOffset
	delete(g.Pending, offset) //已处理，从队列中删除
	return nil
}

func (b *Broker) startRedeliveryLoop() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			b.Mu.RLock()
			topics := make([]*Topic, 0, len(b.Topics))
			for _, t := range b.Topics {
				topics = append(topics, t)
			}
			b.Mu.RUnlock()

			now := time.Now()
			for _, t := range topics {
				t.Mu.Lock()
				for _, g := range t.GroupStates {
					for id, deliveredAt := range g.Pending {
						if now.Sub(deliveredAt) > t.DeliveryTimeout {
							delete(g.Pending, id)
						}
					}
				}
				t.Mu.Unlock()
			}
		}
	}()
}

func errorResponse(msg string) Response {
	return Response{Status: StatusError, Error: msg}
}
