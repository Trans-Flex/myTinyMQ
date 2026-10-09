package main

import "sync"

type Topic struct {
	Name        string
	Messages    []Message
	NextOffset  int64
	AckedOffset int64
	Mu          sync.Mutex
	FilePath    string
	MetaPath    string
}
