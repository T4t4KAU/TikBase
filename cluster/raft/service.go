package raft

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"net/rpc"
)

type JoinService struct {
	peer   *Peer
	nodeId string
	poll   *poll.NetPoll
}

type JoinArgs struct {
	NodeId      string
	ServiceAddr string
	RaftAddr    string
}

type JoinReply struct {
	Success bool
}

func NewJoinService(nodeId, addr string, peer *Peer) (*JoinService, error) {
	p, err := poll.New(poll.Config{
		Address:    addr,
		MaxConnect: 10,
		Timeout:    raftTimeout,
	}, &JoinServiceHandler{})
	if err != nil {
		return nil, err
	}

	return &JoinService{
		peer:   peer,
		nodeId: nodeId,
		poll:   p,
	}, nil
}

func (s *JoinService) AddNode(args JoinArgs, reply *JoinReply) error {
	err := s.peer.Join(args.NodeId, args.ServiceAddr, args.RaftAddr)
	if err != nil {
		reply.Success = false
		return err
	}
	reply.Success = true
	return nil
}

func (s *JoinService) Start() error {
	err := rpc.Register(s)
	if err != nil {
		return err
	}
	return s.poll.Run()
}

type JoinServiceHandler struct{}

func (h *JoinServiceHandler) Handle(conn iface.Connection) {
	rpc.ServeConn(conn)
}
