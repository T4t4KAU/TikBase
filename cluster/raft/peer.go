package raft

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/bytedance/sonic"
	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
)

type Peer struct {
	Id       string
	Address  string
	raftNode *raft.Raft
	fsm      *FSM
	option   Option
	store    iface.Engine
}

func NewPeer(option Option, localId string, fsm FSM) (*Peer, error) {
	logs, err := bolt.NewBoltStore(filepath.Join(option.DirPath, "raft-bolt.db"))
	if err != nil {
		return &Peer{}, err
	}
	shot, err := raft.NewFileSnapshotStore(option.DirPath, option.SnapshotCount, os.Stderr)
	if err != nil {
		return &Peer{}, err
	}

	addr, err := net.ResolveTCPAddr("tcp", option.Address)
	if err != nil {
		return &Peer{}, err
	}
	transport, err := raft.NewTCPTransport(option.Address, addr, option.MaxPool, option.Timeout, os.Stderr)
	if err != nil {
		return &Peer{}, err
	}

	eng, err := engine.NewEngine("bases")
	if err != nil {
		return &Peer{}, err
	}

	stable := NewStableStore(eng)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localId)
	node, err := raft.NewRaft(config, &fsm, logs, stable, shot, transport)

	return &Peer{
		raftNode: node,
		fsm:      &fsm,
		option:   option,
		Id:       localId,
		Address:  addr.String(),
	}, nil
}

func (peer *Peer) Bootstrap() {
	peer.raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(peer.Id),
				Address: raft.ServerAddress(peer.Address),
			},
		},
	})
}

func (peer *Peer) LeaderAddr() string {
	return string(peer.raftNode.Leader())
}

func (peer *Peer) LeaderID() (string, error) {
	addr := peer.LeaderAddr()
	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		return "", err
	}

	servers := peer.raftNode.GetConfiguration().Configuration().Servers
	for _, s := range servers {
		if s.Address == raft.ServerAddress(addr) {
			return string(s.ID), nil
		}
	}

	return "", nil
}

func (peer *Peer) Set(key string, val []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.SET_STR,
		Key:   key,
		Value: val,
	}
	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) Get(key string, level ConsistencyLevel) ([]byte, error) {
	if peer.raftNode.State() != raft.Leader {
		return []byte{}, raft.ErrNotLeader
	}

	if level == Consistent {

	}

	args := [][]byte{utils.S2B(key)}
	res := peer.store.Exec(iface.GET_STR, args)
	return res.Data(), res.Error()
}

func (peer *Peer) Del(key string) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins: iface.DEL,
		Key: key,
	}
	b, err := c.Encode()
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) HSet(key, field string, val []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.SET_STR,
		Key:   key,
		Field: field,
		Value: val,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) HDel(key, field string) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.SET_STR,
		Key:   key,
		Field: field,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) SAdd(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.ADD_SET,
		Key:   key,
		Value: element,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) SRem(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.REM_SET,
		Key:   key,
		Value: element,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) LPush(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.LEFT_PUSH_LIST,
		Key:   key,
		Value: element,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) LPop(key string, element []byte) ([]byte, error) {
	if peer.raftNode.State() != raft.Leader {
		return nil, raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.LEFT_POP_LIST,
		Key:   key,
		Value: element,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return nil, err
	}

	res := peer.raftNode.Apply(b, peer.option.Timeout).(iface.Result)
	return res.Data(), res.Error()
}

func (peer *Peer) RPush(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.RIGHT_PUSH_LIST,
		Key:   key,
		Value: element,
	}

	b, err := sonic.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, peer.option.Timeout).Error()
}

func (peer *Peer) Join(nodeId, httpAddr, addr string) error {
	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		return err
	}

	for _, s := range config.Configuration().Servers {
		if s.ID == raft.ServerID(nodeId) || s.Address == raft.ServerAddress(addr) {
			return nil
		}

		future := peer.raftNode.RemoveServer(s.ID, 0, 0)
		if err := future.Error(); err != nil {
			return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, addr, err)
		}
	}

	f := peer.raftNode.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	return nil
}

func (peer *Peer) SetMeta(key, value string) error {
	return peer.Set(key, utils.S2B(value))
}

func (peer *Peer) GetMeta(key string) (string, error) {
	val, err := peer.Get(key, Stale)
	return utils.B2S(val), err
}

func (peer *Peer) DelMeta(key string) error {
	return peer.Del(key)
}

func pathExists(path string) bool {
	if _, err := os.Lstat(path); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
