package raft

import (
	"encoding/json"
	"fmt"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
	openTimeout         = 60 * time.Second
)

// Peer Raft_节点
type Peer struct {
	id        string // 节点ID
	raftNode  *raft.Raft
	store     iface.Engine // 存储引擎
	dirPath   string       // 日志存储路径
	address   string       // 通信地址
	snapCount int          // 快照数目
	maxPool   int
	single    bool // 单节点
}

type FSM Peer

// NewPeer 创建节点
func NewPeer(option Option, id string, eng iface.Engine) (*Peer, error) {
	return &Peer{
		id:        id,
		store:     eng,
		address:   option.RaftBind,
		dirPath:   option.RaftDir,
		snapCount: option.SnapshotCount,
		maxPool:   option.MaxPool,
		single:    option.Single,
	}, nil
}

func (peer *Peer) Apply(c iface.Command) error {
	b, err := marshal(c)
	if err != nil {
		return err
	}
	return peer.raftNode.Apply(b, raftTimeout).Error()
}

// Engine 返回存储引擎
func (peer *Peer) Engine() iface.Engine {
	return peer.store
}

// ID 返回节点ID
func (peer *Peer) ID() string {
	return peer.id
}

func (peer *Peer) State() raft.RaftState {
	return peer.raftNode.State()
}

// Bootstrap 节点启动
func (peer *Peer) Bootstrap() error {
	localId := peer.id

	config := raft.DefaultConfig()                     // 使用默认配置
	config.LocalID = raft.ServerID(localId)            // 本地节点ID
	raftPath := filepath.Join(peer.dirPath, "raft.db") // raft日志存储路径
	config.LogOutput = os.Stdout

	newNode := !utils.PathExists(raftPath) // 检查路径是否存在 如果不存在则说明是新节点
	addr, err := net.ResolveTCPAddr("tcp", peer.address)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(peer.address, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// 创建文件快照
	snapshots, err := raft.NewFileSnapshotStore(peer.dirPath, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	var logStore raft.LogStore // 日志存储
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(raftPath) // 初始化数据访问接口
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	logStore = boltDB
	stableStore = boltDB

	// 创建状态机
	ra, err := raft.NewRaft(config, (*FSM)(peer), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	peer.raftNode = ra

	// 单节点启动
	if peer.single && newNode {
		klog.Infof("bootstrap needed")
		conf := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		// 启动集群
		ra.BootstrapCluster(conf)
	} else {
		klog.Infof("no bootstrap needed")
	}

	return nil
}

func (peer *Peer) ReplicaList() string {
	servers := peer.raftNode.GetConfiguration().Configuration().Servers
	data, err := json.Marshal(servers)
	if err != nil {
		return ""
	}
	return utils.B2S(data)
}

// LeaderAddr 返回主节点地址
func (peer *Peer) LeaderAddr() string {
	return string(peer.raftNode.Leader())
}

// LeaderID 返回领导者节点ID
func (peer *Peer) LeaderID() (string, error) {
	addr := peer.LeaderAddr()
	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		klog.Errorf("failed to get raft configuration: %v", err)
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
	// 创建定时器
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
	// 创建定时器
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

	klog.Infof("waiting for up to %s for application of initial logs", timeout)
	if err := peer.WaitForAppliedIndex(peer.raftNode.LastIndex(), timeout); err != nil {
		return errno.ErrRaftOpenTimeout
	}
	return nil
}

// 一致性读
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

	// 创建SET命令
	c := &iface.Command{
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

func (peer *Peer) Del(key string) error {
	if peer.raftNode.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	// 创建DEL命令
	c := &iface.Command{
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

func (peer *Peer) Get(key string, level ConsistencyLevel) (string, error) {
	if level != Stale {
		if peer.raftNode.State() != raft.Leader {
			return "", raft.ErrNotLeader
		}
	}

	if level == Consistent {
		if err := peer.consistentRead(); err != nil {
			return "", err
		}
	}

	keyBytes := utils.KeyBytes(key)
	res := peer.Engine().Exec(iface.GET_STR, keyBytes)
	return res.String(), res.Error()
}

// Join 节点加入集群
func (peer *Peer) Join(nodeId, serviceAddr, raftAddr string) error {
	klog.Infof("received join request for remote node %s at %s", nodeId, raftAddr)

	config := peer.raftNode.GetConfiguration()
	if err := config.Error(); err != nil {
		klog.Infof("failed to get raft configuration: %v", err)
		return err
	}

	// 遍历服务器
	for _, s := range config.Configuration().Servers {
		// 节点已经存在
		if s.ID == raft.ServerID(nodeId) || s.Address == raft.ServerAddress(raftAddr) {
			if s.Address == raft.ServerAddress(raftAddr) && s.ID == raft.ServerID(nodeId) {
				klog.Infof("node %s at %s already member of cluster, ignoring join request", nodeId, raftAddr)
				return nil
			}

			// 移除节点
			future := peer.raftNode.RemoveServer(s.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, raftAddr, err)
			}
		}
	}

	// 添加新节点
	f := peer.raftNode.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(raftAddr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	// 存储元数据
	if err := peer.SetMeta(nodeId, serviceAddr); err != nil {
		return err
	}

	klog.Infof("node %s at %s joined successfully", nodeId, raftAddr)
	return nil
}

// SetMeta 设置元数据
func (peer *Peer) SetMeta(key, value string) error {
	return peer.Set(key, utils.S2B(value))
}

// DelMeta 删除元数据
func (peer *Peer) DelMeta(key string) error {
	return peer.Del(key)
}

func (peer *Peer) GetMeta(key string) (string, error) {
	return peer.Get(key, Stale)
}

func (peer *Peer) LeaderAPIAddr() string {
	id, err := peer.LeaderID()
	if err != nil {
		return ""
	}

	addr, err := peer.GetMeta(id)
	if err != nil {
		return ""
	}

	return addr
}
