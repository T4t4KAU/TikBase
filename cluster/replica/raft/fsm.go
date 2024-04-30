package raft

import (
	"encoding/json"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/hashicorp/raft"
	"io"
)

// Apply 应用日志项
func (fsm *FSM) Apply(entry *raft.Log) any {
	var c command

	// 反序列化数据 获取执行命令
	if err := unmarshal(entry.Data, &c); err != nil {
		klog.Errorf("failed to unmarshal command: %s", err.Error())
	}

	// 执行命令
	return fsm.store.Exec(c.Ins, c.Args()).Error()
}

// Snapshot 状态机快照
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := fsm.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		data: data,
	}, nil
}

// Restore 从快照恢复数据
func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	data := make([]byte, 0)
	_, err := snapshot.Read(data)
	if err != nil {
		return err
	}
	return fsm.store.RecoverFromBytes(data)
}

func unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
