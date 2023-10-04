package raft

import "sync"

// Node 节点
type Node struct {
	Mutex sync.Mutex
	Dead  int32

	CurrentTerm int
	VotedFor    int
	Log         []string
	CommitIndex int
	LastApplied int
	NextIndex   []int
	MatchIndex  []int
}

func (node *Node) GetState() {

}
