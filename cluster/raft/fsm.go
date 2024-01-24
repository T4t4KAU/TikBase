package raft

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/bytedance/sonic"
	"github.com/hashicorp/raft"
	"io"
)

type LogEntry struct {
	Key   string
	Value string
}

func (fsm *FSM) Apply(entry *raft.Log) any {
	var c command
	if err := sonic.Unmarshal(entry.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	args := utils.KeyValueBytes(c.Key, c.Value)
	return fsm.store.Exec(c.Ins, args).Error()
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &Snapshot{}, nil
}

func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}
