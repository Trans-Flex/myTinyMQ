package main

import (
	"sync"
	"time"
)

type GroupState struct {
	AckedOffset    int64
	NextReadOffset int64
	Pending        map[int64]time.Time
}

type Topic struct {
	Name            string
	Messages        []Message
	NextOffset      int64
	GroupStates     map[string]*GroupState
	Mu              sync.Mutex
	FilePath        string
	MetaPath        string
	DeliveryTimeout time.Duration
}
