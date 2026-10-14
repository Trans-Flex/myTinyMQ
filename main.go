package main

import (
	"log"
	"time"
)

func main() {
	b := NewBroker("data", 5*time.Second, 100)
	if err := b.loadFromDisk(); err != nil {
		log.Fatalf("加载磁盘数据失败: %v", err)
	}
	log.Println("broker started on :9092")
	b.startRedeliveryLoop()
	if err := b.Start(":9092"); err != nil {
		log.Fatal(err)
	}
}
