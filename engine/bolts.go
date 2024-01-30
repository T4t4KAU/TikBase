package engine

import (
	"github.com/T4t4KAU/TikBase/iface"
)

type BoltEngine struct {
}

func (b *BoltEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	//TODO implement me
	panic("implement me")
}

func (b *BoltEngine) Snapshot() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BoltEngine) RecoverFromBytes(data []byte) error {
	//TODO implement me
	panic("implement me")
}
