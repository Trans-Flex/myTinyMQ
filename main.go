package main

import "log"

func main() {
	b := &Broker{
		Topics: make(map[string]*Topic),
	}
	if err := b.loadFromDisk(); err != nil {
		log.Fatalf("加载磁盘数据失败: %v", err)
	}
	log.Println("broker started on :9092")
	if err := b.Start(":9092"); err != nil {
		log.Fatal(err)
	}
}
