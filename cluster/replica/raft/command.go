package raft

import (
	"encoding/json"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

// Raft 状态机指令
type command struct {
	Ins   iface.INS `json:"op,omitempty"`
	Key   string    `json:"key,omitempty"`
	Field string    `json:"field,omitempty"`
	Value []byte    `json:"value,omitempty"`
}

// ConsistencyLevel 一致性级别
type ConsistencyLevel int

const (
	Default ConsistencyLevel = iota
	Stale
	Consistent
)

// Encode 将指令编码
func (c command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c command) Args() [][]byte {
	return utils.KeyValueBytes(c.Key, c.Value)
}
