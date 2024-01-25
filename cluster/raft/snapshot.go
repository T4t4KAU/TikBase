package raft

import (
	"github.com/bytedance/sonic"
	"github.com/hashicorp/raft"
)

type Snapshot struct {
	data []byte
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, e := sonic.Marshal(s.data)
		if e != nil {
			return e
		}

		if _, e = sink.Write(b); e != nil {
			return e
		}

		return sink.Close()
	}()

	if err != nil {
		_ = sink.Cancel()
		return err
	}

	return nil
}

func (s *Snapshot) Release() {
}
