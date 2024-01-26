package raft

import (
	"github.com/T4t4KAU/TikBase/pkg/net/rpc"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"net"
)

type Service struct {
	lis     net.Listener
	peer    *Peer
	addr    string
	nodeId  string
	reactor poll.Reactor
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) Start() error {
	svc, err := NewJoinService(s.nodeId, s.peer)
	if err != nil {
		return err
	}

	err = rpc.Register(svc)
	if err != nil {
		return err
	}

	ch := make(chan struct{})
	s.reactor.Run(s.lis, ch)

	return nil
}

type JoinService struct {
	peer *Peer
}

type JoinArgs struct {
	NodeId      string
	ServiceAddr string
	RaftAddr    string
}

type JoinReply struct {
	Success bool
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

func NewJoinService(nodeId string, peer *Peer) (*JoinService, error) {
	return &JoinService{
		peer: peer,
	}, nil
}
