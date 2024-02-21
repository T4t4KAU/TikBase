package region

import (
	"github.com/T4t4KAU/TikBase/cluster/consis"
	"github.com/T4t4KAU/TikBase/cluster/consis/raft"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"sync"
	"time"
)

type Region struct {
	services map[string]iface.IService
}

func New(config *config.RegionConfig, eng iface.Engine) (*Region, error) {
	re := &Region{}

	peer, err := raft.NewPeer(raft.Option{
		RaftDir:       config.DirPath,
		RaftBind:      config.Address,
		MaxPool:       config.WorkerNum,
		SnapshotCount: config.SnapshotCount,
		Timeout:       time.Duration(config.Timeout),
		Single:        config.JoinAddr == "",
	}, config.Id, eng)
	if err != nil {
		return &Region{}, err
	}

	re.registerService("consis-service", consis.NewService(peer, config.ServiceAddr))

	return re, nil
}

func (r *Region) registerService(name string, service iface.IService) {
	r.services[name] = service
}

func (r *Region) Start() {
	var wg sync.WaitGroup

	for _, svc := range r.services {
		wg.Add(1)
		go func(service iface.IService) {
			wg.Done()
			_ = service.Start()
		}(svc)
	}

	wg.Wait()
}
