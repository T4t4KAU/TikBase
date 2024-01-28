package web

import (
	"github.com/T4t4KAU/TikBase/cluster/raft"
	"github.com/T4t4KAU/TikBase/pkg/net/http"
)

type Service struct {
	Address string
	Peer    *raft.Peer
}

func NewService(addr string, peer *raft.Peer) *Service {
	return &Service{
		Address: addr,
		Peer:    peer,
	}
}

func (s *Service) Start() error {
	return http.StartServer(s.Address, s.Peer.Engine())
}
