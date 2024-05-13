package replica

import (
	"context"
	"github.com/T4t4KAU/TikBase/cluster/replica/raft"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/consts"
	"github.com/T4t4KAU/TikBase/pkg/rpc/replica"
	"github.com/T4t4KAU/TikBase/pkg/rpc/replica/replicaservice"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"net"
	"time"
)

const (
	joinDealyTime = 500 * time.Millisecond
)

type Service struct {
	peer    *raft.Peer
	address string
	config  *config.ReplicaConfig
}

func (s *Service) GetId(ctx context.Context, req *replica.GetIdReq) (r *replica.GetIdResp, err error) {
	r.NodeId = s.peer.ID()
	return
}

func (s *Service) ReplicaList() string {
	return s.peer.ReplicaList()
}

func NewService(peer *raft.Peer, addr string, config *config.ReplicaConfig) *Service {
	return &Service{
		peer:    peer,
		address: addr,
		config:  config,
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

// LeaderAddr implements the ReplicaServiceImpl interface.
func (s *Service) LeaderAddr(ctx context.Context, req *replica.LeaderAddrReq) (resp *replica.LeaderAddrResp, err error) {
	resp = new(replica.LeaderAddrResp)
	resp.Address = s.peer.LeaderAddr()

	return
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

	go func() {
		_ = srv.Run()
	}()

	time.Sleep(joinDealyTime)

	if s.config.JoinAddr != "" {
		err = join(s.config.RaftAddr, s.config.ServiceAddr, s.peer.ID(), s.config.JoinAddr)
	}

	return err

}

func (s *Service) Name() string {
	return consts.ReplicaServiceName
}

func join(raftAddr, serviceAddr, nodeId string, joinAddr string) error {
	cli, err := replicaservice.NewClient(consts.ReplicaServiceName, client.WithHostPorts(joinAddr))
	if err != nil {
		return err
	}
	_, err = cli.Join(context.Background(), &replica.JoinReq{
		RaftAddr:    raftAddr,
		ServiceAddr: serviceAddr,
		NodeId:      nodeId,
	})

	if err != nil {
		klog.Errorf("failed to join: error=%v", err)
		return err
	}

	return nil
}
