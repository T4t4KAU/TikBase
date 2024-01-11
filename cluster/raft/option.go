package raft

import "time"

type Option struct {
	DirPath       string
	Address       string
	MaxPool       int
	SnapshotCount int
	Timeout       time.Duration
}
