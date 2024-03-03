package replica

import (
	"context"
	"github.com/T4t4KAU/TikBase/cluster/replica/raft"
	"github.com/T4t4KAU/TikBase/pkg/rpc/replica"
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
func (s *Service) Join(ctx context.Context, req *replica.JoinReq) (resp *replica.JoinResp, err error) {
	resp = new(replica.JoinResp)

	err = s.peer.Join(req.NodeId, req.ServiceAddr, req.RaftAddr)
	if err != nil {
		resp.Message = err.Error()
		return resp, err
	}

	return resp, nil
}
