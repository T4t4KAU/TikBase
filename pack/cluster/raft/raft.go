package raft

import "sync"

// Raft raft节点
type Raft struct {
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

func (rf *Raft) GetState() {

}
