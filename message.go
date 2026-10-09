package main

import "time"

type Message struct {
	ID        int64
	Topic     string
	Body      string
	Timestamp time.Time
}
