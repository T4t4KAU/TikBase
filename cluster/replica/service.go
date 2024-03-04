package replica

import (
	"context"
	"github.com/T4t4KAU/TikBase/cluster/replica/raft"
	"github.com/T4t4KAU/TikBase/pkg/rpc/replica"
	"github.com/T4t4KAU/TikBase/pkg/rpc/replica/replicaservice"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"net"
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

// Start 启动服务
func (s *Service) Start() error {
	addr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	err = s.peer.Bootstrap()
	if err != nil {
		return err
	}

	srv := replicaservice.NewServer(s,
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: s.Name()}),
		server.WithServiceAddr(addr),
	)

	klog.Infof("start replica service at %s", s.address)

	return srv.Run()
}

func (s *Service) Name() string {
	return "replica-service"
}
