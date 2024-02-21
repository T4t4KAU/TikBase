package consis

import (
	"context"
	"github.com/T4t4KAU/TikBase/cluster/consis/raft"
	consis "github.com/T4t4KAU/TikBase/pkg/rpc/consis"
)

type Service struct {
	peer    *raft.Peer
	address string
}

func NewService(peer *raft.Peer, addr string) *Service {
	return &Service{
		peer:    peer,
		address: addr,
	}
}

// Join implements the ConsisServiceImpl interface.
func (s *Service) Join(ctx context.Context, req *consis.JoinReq) (resp *consis.JoinResp, err error) {
	resp = new(consis.JoinResp)

	err = s.peer.Join(req.NodeId, req.ServiceAddr, req.RaftAddr)
	if err != nil {
		resp.Message = err.Error()
		return resp, err
	}

	return resp, nil
}
