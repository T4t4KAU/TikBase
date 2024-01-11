package raft

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/bytedance/sonic"
)

type command struct {
	Ins   iface.INS `json:"op,omitempty"`
	Key   string    `json:"key,omitempty"`
	Field string    `json:"field,omitempty"`
	Value []byte    `json:"value,omitempty"`
}

type ConsistencyLevel int

const (
	Default ConsistencyLevel = iota
	Stale
	Consistent
)

func (c command) Encode() ([]byte, error) {
	return sonic.Marshal(c)
}
