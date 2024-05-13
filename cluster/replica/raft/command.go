package raft

// ConsistencyLevel 一致性级别
type ConsistencyLevel int

const (
	Default ConsistencyLevel = iota
	Stale
	Consistent
)
