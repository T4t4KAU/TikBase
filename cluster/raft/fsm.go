package raft

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/bytedance/sonic"
	"github.com/hashicorp/raft"
	"io"
)

func (fsm *FSM) Apply(entry *raft.Log) any {
	var c command
	if err := sonic.Unmarshal(entry.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	args := utils.KeyValueBytes(c.Key, c.Value)
	return fsm.store.Exec(c.Ins, args).Error()
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := fsm.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		data: data,
	}, nil
}

func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	data := make([]byte, 0)
	_, err := snapshot.Read(data)
	if err != nil {
		return err
	}
	return fsm.store.RecoverFromBytes(data)
}
