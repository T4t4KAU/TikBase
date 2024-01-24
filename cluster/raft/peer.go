package raft

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/bytedance/sonic"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	FileName = "raft.db"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
	openTimeout         = 60 * time.Second
)

type Peer struct {
	id        string
	raftNode  *raft.Raft
	store     iface.Engine
	dirPath   string
	address   string
	snapCount int
	maxPool   int
}

type FSM Peer

// NewPeer 创建节点
func NewPeer(option Option, id string, fsm FSM) (*Peer, error) {
	eng, err := engine.NewEngine(option.Store)
	if err != nil {
		return nil, err
	}

	return &Peer{
		id:        id,
		store:     eng,
		address:   option.RaftBind,
		dirPath:   option.RaftDir,
		snapCount: option.SnapshotCount,
		maxPool:   option.MaxPool,
	}, nil
}

// ID 返回节点ID
func (peer *Peer) ID() string {
	return peer.id
}

// Bootstrap 节点启动
func (peer *Peer) Bootstrap(single bool, localId string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localId)

	newNode := !pathExists(filepath.Join(peer.dirPath, "raft.db"))
	addr, err := net.ResolveTCPAddr("tcp", peer.address)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(peer.address, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(peer.dirPath, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(peer.dirPath, "raft.db"))
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	ra, err := raft.NewRaft(config, (*FSM)(peer), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	peer.raftNode = ra

	if single && newNode {
		conf := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(conf)
	} else {

	}

	return nil
}

func pathExists(path string) bool {
	if _, err := os.Lstat(path); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
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
	for _, server := range servers {
		if server.Address == raft.ServerAddress(addr) {
			return string(server.ID), nil
		}
	}

	return "", nil
}

// WaitForLeader 阻塞直到发现一个leader
func (peer *Peer) WaitForLeader(timeout time.Duration) (string, error) {
	ticker := time.NewTicker(leaderWaitDelay)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			addr := peer.LeaderAddr()
			if addr != "" {
				return addr, nil
			}
		case <-timer.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

// WaitForAppliedIndex 阻塞直到一个日志项被应用
func (peer *Peer) WaitForAppliedIndex(index uint64, timeout time.Duration) error {
	ticker := time.NewTicker(appliedWaitDelay)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if peer.raftNode.AppliedIndex() >= index {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

func (peer *Peer) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	if err := peer.WaitForAppliedIndex(peer.raftNode.LastIndex(), timeout); err != nil {
		return err
	}
	return nil
}

func (peer *Peer) consistentRead() error {
	future := peer.raftNode.VerifyLeader()
	if err := future.Error(); err != nil {
		return err
	}
	return nil
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

func (peer *Peer) Get(key string, level ConsistencyLevel) ([]byte, error) {
	if peer.raftNode.State() != raft.Leader {
		return []byte{}, raft.ErrNotLeader
	}

	if level == Consistent {
		if err := peer.consistentRead(); err != nil {
			return []byte{}, err
		}
	}

	args := utils.KeyBytes(key)
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
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

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
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

	err = peer.raftNode.Apply(b, raftTimeout).Error()
	if err != nil {
		return nil, err
	}

	return peer.Get(key, Default)
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

	return peer.raftNode.Apply(b, raftTimeout).Error()
}

// Join 节点加入集群
func (peer *Peer) Join(nodeId, httpAddr, addr string) error {
	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		return err
	}

	for _, s := range config.Configuration().Servers {
		// 节点已经存在
		if s.ID == raft.ServerID(nodeId) || s.Address == raft.ServerAddress(addr) {
			return nil
		}

		future := peer.raftNode.RemoveServer(s.ID, 0, 0)
		if err := future.Error(); err != nil {
			return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, addr, err)
		}
	}

	// 追加节点
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
