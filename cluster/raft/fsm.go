package raft

import (
	"encoding/json"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/hashicorp/raft"
	"io"
)

type FSM struct {
	engine   iface.Engine
	raft     *raft.Raft
	notifyCh chan bool
}

type LogEntry struct {
	Key   string
	Value string
}

func (fsm *FSM) Apply(entry *raft.Log) any {
	en := LogEntry{}
	if err := json.Unmarshal(entry.Data, &en); err != nil {
		panic("failed to unmarshal raft log entry")
	}

	return nil
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}
