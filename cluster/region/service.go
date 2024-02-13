package region

import (
	"github.com/T4t4KAU/TikBase/cluster/raft"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/tlog"
	"sync"
)

type Service struct {
	services map[string]iface.IService
	closeCh  chan struct{}
}

// New 创建服务
func New(nodeId string, eng iface.Engine, conf config.ReplicaConfig) *Service {
	svc := &Service{
		services: make(map[string]iface.IService),
		closeCh:  make(chan struct{}, 1),
	}

	// 创建peer
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

	// 注册服务
	svc.Register("raft-service", raft.NewService(nodeId, conf.ServiceAddr, peer))

	if conf.JoinAddr != "" {
		// 加入节点
		succ, err := raft.AddNode(nodeId, conf.ServiceAddr, conf.Address, conf.TargetAddr)
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
	var wg sync.WaitGroup

	for _, svc := range s.services {
		wg.Add(1)
		go func(service iface.IService) {
			defer wg.Done()
			err := service.Start()
			if err != nil {
				tlog.Errorf("failed to start service: name=%s error=%v\n", service.Name(), err)
			}
		}(svc)
	}

	wg.Wait()
}

// Register 注册服务
func (s *Service) Register(name string, service iface.IService) {
	s.services[name] = service
}

// Remove 移除服务
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
