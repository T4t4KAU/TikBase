package region

import (
	"github.com/T4t4KAU/TikBase/cluster/data"
	"github.com/T4t4KAU/TikBase/cluster/replica"
	"github.com/T4t4KAU/TikBase/cluster/replica/raft"
	"github.com/T4t4KAU/TikBase/cluster/slice"
	"github.com/T4t4KAU/TikBase/cluster/txn"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/cloudwego/kitex/pkg/klog"
	"strconv"
	"sync"
	"time"
)

type Region struct {
	services map[string]iface.IService
	txm      *txn.TxManager
	*slice.Slice
}

func New(replicaConfig *config.ReplicaConfig, sliceConfig *config.SliceConfig, eng iface.Engine) (*Region, error) {
	re := &Region{
		services: make(map[string]iface.IService),
	}

	// 创建节点
	peer, err := raft.NewPeer(raft.Option{
		RaftDir:       replicaConfig.DirPath,
		RaftBind:      replicaConfig.Address,
		MaxPool:       replicaConfig.WorkerNum,
		SnapshotCount: replicaConfig.SnapshotCount,
		Timeout:       time.Duration(replicaConfig.Timeout),
		Single:        replicaConfig.JoinAddr == "",
	}, replicaConfig.Id, eng)
	if err != nil {
		return &Region{}, err
	}

	re.Slice, err = slice.New(slice.Options{
		Name:                 sliceConfig.Id,
		Address:              sliceConfig.Address,
		ServerType:           "tcp",
		VirtualNodeCount:     sliceConfig.VirtualNodeCount,
		UpdateCircleDuration: slice.DefaultOptions.UpdateCircleDuration,
		Cluster:              []string{sliceConfig.JoinAddr},
	}, eng)

	if err != nil {
		return &Region{}, err
	}

	/// 注册服务
	re.registerService("replica-service", replica.NewService(peer, replicaConfig.ServiceAddr))
	re.registerService("data-service", data.NewService(re.Slice, ":"+strconv.Itoa(sliceConfig.ServicePort)))

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

	klog.Info("all service start")
}
