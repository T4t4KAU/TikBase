package raft

import "github.com/hashicorp/raft"

type snapshot struct {
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	//TODO implement me
	panic("implement me")
}

func (s *snapshot) Release() {
}
