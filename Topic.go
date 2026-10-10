package main

import (
	"sync"
	"time"
)

type Topic struct {
	Name            string
	Messages        []Message
	NextOffset      int64
	AckedOffset     int64
	Mu              sync.Mutex
	FilePath        string
	MetaPath        string
	Pending         map[int64]time.Time
	DeliveryTimeout time.Duration
}
