package raft

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"net/rpc"
)

type Service struct {
	peer   *Peer
	nodeId string
	poll   *poll.NetPoll
}

type Args struct {
	NodeId   string
	JoinAddr string
	RaftAddr string
}

type Reply struct {
	Success bool
}

// NewService 创建共识服务
func NewService(nodeId, addr string, peer *Peer) *Service {
	_, port := utils.SplitAddressAndPort(addr)
	po, err := poll.New(poll.Config{
		Address:    "127.0.0.1:" + port,
		MaxConnect: 20,
		Timeout:    raftTimeout,
	}, &ServiceHandler{})
	if err != nil {
		panic(err)
	}

	return &Service{
		peer:   peer,
		nodeId: nodeId,
		poll:   po,
	}
}

// AddNode 添加节点
func (s *Service) AddNode(args Args, reply *Reply) error {
	err := s.peer.Join(args.NodeId, args.JoinAddr, args.RaftAddr)
	if err != nil {
		reply.Success = false
		return err
	}
	reply.Success = true
	return nil
}

// Start 启动服务
func (s *Service) Start() error {
	fmt.Printf("start raft service at %s...\n", s.peer.address)
	err := rpc.RegisterName("RaftService", s)
	if err != nil {
		return err
	}

	// 启动节点
	err = s.peer.Bootstrap(s.nodeId)
	if err != nil {
		return err
	}

	// 开启RPC服务监听
	return s.poll.Run()
}

func (s *Service) Close() {
	s.poll.Close()
}

type ServiceHandler struct{}

// Handle 处理连接
func (h *ServiceHandler) Handle(conn iface.Connection) {
	rpc.ServeConn(conn)
}
