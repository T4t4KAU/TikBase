package raft

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"net/rpc"
)

type Service struct {
	peer   *Peer
	nodeId string
	poll   *poll.NetPoll
}

type Args struct {
	NodeId      string
	ServiceAddr string
	RaftAddr    string
}

type Reply struct {
	Success bool
}

// NewService 创建共识服务
func NewService(nodeId, addr string, peer *Peer) *Service {
	p, err := poll.New(poll.Config{
		Address:    addr,
		MaxConnect: 10,
		Timeout:    raftTimeout,
	}, &ServiceHandler{})
	if err != nil {
		panic(err)
	}

	return &Service{
		peer:   peer,
		nodeId: nodeId,
		poll:   p,
	}
}

// AddNode 添加节点
func (s *Service) AddNode(args Args, reply *Reply) error {
	err := s.peer.Join(args.NodeId, args.ServiceAddr, args.RaftAddr)
	if err != nil {
		reply.Success = false
		return err
	}
	reply.Success = true
	return nil
}

// Start 启动服务
func (s *Service) Start() error {
	err := rpc.Register(s)
	if err != nil {
		return err
	}
	return s.poll.Run()
}

type ServiceHandler struct{}

// Handle 处理连接
func (h *ServiceHandler) Handle(conn iface.Connection) {
	rpc.ServeConn(conn)
}
