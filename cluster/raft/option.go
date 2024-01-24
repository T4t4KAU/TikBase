package raft

import "time"

type Option struct {
	RaftDir       string
	RaftBind      string
	MaxPool       int
	SnapshotCount int
	Timeout       time.Duration
	Store         string
}

var DefaultOption = Option{
	RaftDir:       "raft",
	RaftBind:      "127.0.0.1",
	MaxPool:       10,
	SnapshotCount: 10,
	Timeout:       raftTimeout,
	Store:         "BASES",
}
