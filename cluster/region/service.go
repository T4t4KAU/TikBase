package region

import (
	"github.com/T4t4KAU/TikBase/cluster/raft"
	"github.com/T4t4KAU/TikBase/cluster/rpc"
	"github.com/T4t4KAU/TikBase/cluster/web"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/tlog"
)

type Service struct {
	services map[string]iface.IService
	closeCh  chan struct{}
}

// New 创建服务
func New(nodeId, addr string, eng iface.Engine, conf config.ReplicaConfig) *Service {
	svc := &Service{
		services: make(map[string]iface.IService),
		closeCh:  make(chan struct{}, 1),
	}

	peer, err := raft.NewPeer(raft.Option{
		RaftDir:       conf.DirPath,
		RaftBind:      conf.Address,
		MaxPool:       conf.WorkerNum,
		SnapshotCount: conf.SnapshotCount,
		Single:        conf.JoinAddr == "",
	}, conf.Id, eng)
	if err != nil {
		panic(err)
	}

	svc.Register("raft-service", raft.NewService(nodeId, addr, peer))
	svc.Register("web-service", web.NewService(addr, peer.Engine()))

	if conf.JoinAddr != "" {
		succ, err := rpc.AddNode(nodeId, conf.JoinAddr, conf.Address)
		if err != nil {
			tlog.Errorf("failed to join cluster: error=%s", err)
			panic(err)
		}
		if !succ {
			tlog.Errorf("failed to join cluster")
			panic("failed to join cluster")
		}
	}

	return svc
}

// Start 启动服务
func (s *Service) Start() {
	for _, svc := range s.services {
		go func(service iface.IService) {
			_ = service.Start()
		}(svc)
	}
}

// Register 注册服务
func (s *Service) Register(name string, service iface.IService) {
	s.services[name] = service
}

// Remove 删除服务
func (s *Service) Remove(name string) bool {
	if _, ok := s.services[name]; ok {
		delete(s.services, name)
		return true
	}
	return false
}

func (s *Service) Close() {
	s.closeCh <- struct{}{}
}
