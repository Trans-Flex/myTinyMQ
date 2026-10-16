package main

import (
	"path/filepath"
	"sync"
	"time"
)

type GroupState struct {
	AckedOffset int64
	Pending     map[int64]time.Time
	AckedSet    map[int64]bool
}

type Topic struct {
	Name        string
	Messages    []Message
	NextOffset  int64
	GroupStates map[string]*GroupState
	Mu          sync.Mutex
	FilePath    string
	MetaPath    string
}

func NewTopic(name, dataDir string) *Topic {
	topic := &Topic{
		Name:        name,
		NextOffset:  0,
		FilePath:    filepath.Join(dataDir, name+".log"),
		MetaPath:    filepath.Join(dataDir, name+".meta"),
		GroupStates: make(map[string]*GroupState),
	}
	return topic
}
