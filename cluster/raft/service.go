package raft

import (
	"errors"
	"fmt"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/hashicorp/raft"
	"net/rpc"
)

type Service struct {
	peer   *Peer
	nodeId string
	poll   *poll.NetPoll
}

type Args struct {
	NodeId      string
	JoinAddr    string
	RaftAddr    string
	ServiceAddr string
}

type Reply struct {
	Success bool
	Message string
}

// NewService 创建共识服务
func NewService(nodeId, addr string, peer *Peer) *Service {
	_, port := utils.SplitAddressAndPort(addr)
	po, err := poll.New(poll.Config{
		Address:    ":" + port,
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
		if errors.Is(err, raft.ErrNotLeader) {
			leader := s.peer.LeaderAddr()
			succ, err := AddNode(args.NodeId, args.JoinAddr, args.ServiceAddr, args.RaftAddr, leader)
			if err != nil {
				reply.Message = err.Error()
				return err
			}
			reply.Success = succ
			return nil
		}

		reply.Success = false
		reply.Message = err.Error()
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

func (s *Service) Name() string {
	return "RaftService"
}

func (s *Service) Close() {
	s.poll.Close()
}

type ServiceHandler struct{}

// Handle 处理连接
func (h *ServiceHandler) Handle(conn iface.Connection) {
	rpc.ServeConn(conn)
}

// AddNode 发送RPC请求
func AddNode(nodeId, joinAddr, serviceAddr, raftAddr, targetAddr string) (bool, error) {
	cli, err := rpc.Dial("tcp", targetAddr)
	if err != nil {
		return false, err
	}

	args := Args{
		NodeId:      nodeId,
		JoinAddr:    joinAddr,
		RaftAddr:    raftAddr,
		ServiceAddr: serviceAddr,
	}

	var reply Reply
	err = cli.Call("RaftService.AddNode", args, &reply)
	if err != nil {
		return false, err
	}

	return true, nil
}
