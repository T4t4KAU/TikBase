package raft

import (
	"encoding/json"
	"fmt"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/tlog"
	"github.com/T4t4KAU/TikBase/pkg/utils"
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

// Peer 单机节点
type Peer struct {
	id        string // 节点ID
	raftNode  *raft.Raft
	store     iface.Engine // 存储引擎
	dirPath   string       // 日志存储路径
	address   string       // 通信地址
	snapCount int          // 快照数目
	maxPool   int
}

type FSM Peer

// NewPeer 创建节点
func NewPeer(option Option, id string, eng iface.Engine) (*Peer, error) {
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

func (peer *Peer) Engine() iface.Engine {
	return peer.store
}

// ID 返回节点ID
func (peer *Peer) ID() string {
	return peer.id
}

// Bootstrap 节点启动
func (peer *Peer) Bootstrap(single bool, localId string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localId)

	// 检查普及是否存在 如果不存在则说明是新节点
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

	// 单节点启动
	if single && newNode {
		tlog.Infof("bootstrap needed")
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
		tlog.Infof("no bootstrap needed")
	}

	return nil
}

func pathExists(path string) bool {
	if _, err := os.Lstat(path); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

// LeaderAddr 返回主节点地址
func (peer *Peer) LeaderAddr() string {
	return string(peer.raftNode.Leader())
}

// LeaderID 返回主节点ID
func (peer *Peer) LeaderID() (string, error) {
	addr := peer.LeaderAddr()
	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		tlog.Errorf("failed to get raft configuration: %v", err)
		return "", err
	}

	// 遍历所有节点
	servers := peer.raftNode.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		if server.Address == raft.ServerAddress(addr) {
			return string(server.ID), nil
		}
	}

	return "", nil
}

// WaitForLeader 阻塞直到发现一个Leader
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
			// 日志更新结束
			if peer.raftNode.AppliedIndex() >= index {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// WaitForApplied 等待日志应用
func (peer *Peer) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}

	tlog.Infof("waiting for up to %s for application of initial logs", timeout)
	if err := peer.WaitForAppliedIndex(peer.raftNode.LastIndex(), timeout); err != nil {
		return errno.ErrRaftOpenTimeout
	}
	return nil
}

// 一致读
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
	b, err := json.Marshal(c)
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
		Ins:   iface.SET_HASH,
		Key:   key,
		Field: field,
		Value: val,
	}

	b, err := json.Marshal(c)
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
		Ins:   iface.DEL_HASH,
		Key:   key,
		Field: field,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

// SAdd 向集合中添加元素
func (peer *Peer) SAdd(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.ADD_SET,
		Key:   key,
		Value: element,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

// SRem 判断元素在集合中是否存在
func (peer *Peer) SRem(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.REM_SET,
		Key:   key,
		Value: element,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

// LPush 添加新元素
func (peer *Peer) LPush(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.LEFT_PUSH_LIST,
		Key:   key,
		Value: element,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := peer.raftNode.Apply(b, raftTimeout)
	return f.Error()
}

// LPop 从列表中弹出元素
func (peer *Peer) LPop(key string, element []byte) ([]byte, error) {
	if peer.raftNode.State() != raft.Leader {
		return nil, raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.LEFT_POP_LIST,
		Key:   key,
		Value: element,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	err = peer.raftNode.Apply(b, raftTimeout).Error()
	if err != nil {
		return nil, err
	}

	return peer.Get(key, Default)
}

// RPush 向列表右边追加元素
func (peer *Peer) RPush(key string, element []byte) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	c := &command{
		Ins:   iface.RIGHT_PUSH_LIST,
		Key:   key,
		Value: element,
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	return peer.raftNode.Apply(b, raftTimeout).Error()
}

// Join 节点加入集群
func (peer *Peer) Join(nodeId, joinAddr, raftAddr string) error {
	tlog.Infof("received join request for remote node %s at %s", nodeId, raftAddr)

	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		tlog.Infof("failed to get raft configuration: %v", err)
		return err
	}

	for _, s := range config.Configuration().Servers {
		// 节点已经存在
		if s.ID == raft.ServerID(nodeId) || s.Address == raft.ServerAddress(raftAddr) {
			tlog.Infof("node %s at %s already memeber of region, ignoring join request", nodeId, raftAddr)
			return nil
		}

		future := peer.raftNode.RemoveServer(s.ID, 0, 0)
		if err := future.Error(); err != nil {
			return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, raftAddr, err)
		}
	}

	// 追加节点
	f := peer.raftNode.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(raftAddr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	if err := peer.SetMeta(nodeId, joinAddr); err != nil {
		return err
	}

	tlog.Infof("node %s at %s joined successfully", nodeId, raftAddr)
	return nil
}

// SetMeta 设置元数据
func (peer *Peer) SetMeta(key, value string) error {
	return peer.Set(key, utils.S2B(value))
}

// GetMeta 获取元数据
func (peer *Peer) GetMeta(key string) (string, error) {
	val, err := peer.Get(key, Stale)
	return utils.B2S(val), err
}

// DelMeta 删除元数据
func (peer *Peer) DelMeta(key string) error {
	return peer.Del(key)
}
