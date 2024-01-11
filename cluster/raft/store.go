package raft

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

type StableStore struct {
	eng iface.Engine
}

func NewStableStore(eng iface.Engine) *StableStore {
	return &StableStore{
		eng: eng,
	}
}

func (s *StableStore) Set(key []byte, val []byte) error {
	args := [][]byte{key, val}
	res := s.eng.Exec(iface.SET_STR, args)
	return res.Error()
}

func (s *StableStore) Get(key []byte) ([]byte, error) {
	args := [][]byte{key}
	res := s.eng.Exec(iface.GET_STR, args)
	return res.Data(), res.Error()
}

func (s *StableStore) SetUint64(key []byte, val uint64) error {
	args := [][]byte{key, utils.U642B(val)}
	res := s.eng.Exec(iface.SET_STR, args)
	return res.Error()
}

func (s *StableStore) GetUint64(key []byte) (uint64, error) {
	args := [][]byte{key}
	res := s.eng.Exec(iface.GET_STR, args)
	return utils.B2U64(res.Data()), res.Error()
}
