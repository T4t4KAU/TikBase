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

func (c command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c command) Args() [][]byte {
	if c.Ins.BIN() {
		return [][]byte{
			utils.S2B(c.Key),
			c.Value,
		}
	} else if c.Ins.TER() {
		return [][]byte{
			utils.S2B(c.Key),
			utils.S2B(c.Field),
			c.Value,
		}
	} else {
		return [][]byte{
			utils.S2B(c.Key),
		}
	}
}
