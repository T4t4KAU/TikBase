package region

import (
	"github.com/T4t4KAU/TikBase/cluster/raft"
	"github.com/T4t4KAU/TikBase/cluster/web"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
)

type Service struct {
	services map[string]iface.IService
}

func New(nodeId, addr string, eng iface.Engine, conf config.ReplicaConfig) *Service {
	svc := &Service{
		services: make(map[string]iface.IService),
	}

	peer, err := raft.NewPeer(raft.Option{
		RaftDir:       conf.DirPath,
		RaftBind:      conf.Address,
		MaxPool:       conf.WorkerNum,
		SnapshotCount: conf.SnapshotCount,
	}, conf.Id, eng)
	if err != nil {
		panic(err)
	}

	svc.Register("raft-service", raft.NewService(nodeId, addr, peer))
	svc.Register("web-service", web.NewService(addr, peer.Engine()))

	return svc
}

func (s *Service) Start() {
	for _, svc := range s.services {
		go func(service iface.IService) {
			_ = service.Start()
		}(svc)
	}

	select {}
}

// Register 注册服务
func (s *Service) Register(name string, service iface.IService) {
	s.services[name] = service
}

func (s *Service) Remove(name string) bool {
	if _, ok := s.services[name]; ok {
		delete(s.services, name)
		return true
	}
	return false
}
