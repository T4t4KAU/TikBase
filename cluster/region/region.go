package region

import (
	"github.com/T4t4KAU/TikBase/cluster/data"
	"github.com/T4t4KAU/TikBase/cluster/replica"
	"github.com/T4t4KAU/TikBase/cluster/replica/raft"
	"github.com/T4t4KAU/TikBase/cluster/slice"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/consts"
	"github.com/cloudwego/kitex/pkg/klog"
	"strconv"
	"sync"
	"time"
)

// Region 数据分区
type Region struct {
	services map[string]iface.IService
}

func New(replicaConfig *config.ReplicaConfig, serverConfig *config.ServerConfig, eng iface.Engine) (*Region, error) {
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
		Single:        replicaConfig.JoinAddr == "", // 是否单节点
	}, replicaConfig.Id, eng)
	if err != nil {
		return &Region{}, err
	}

	// 创建数据分区
	sc, err := slice.New(slice.Options{
		Name:                 serverConfig.Id,
		Address:              serverConfig.Address,
		ServerType:           "tcp",
		VirtualNodeCount:     serverConfig.VirtualNodeCount,
		UpdateCircleDuration: slice.DefaultOptions.UpdateCircleDuration,
		Cluster:              []string{serverConfig.JoinAddr},
	}, eng)

	if err != nil {
		return &Region{}, err
	}

	/// 注册服务
	re.registerService(consts.ReplicaServiceName, replica.NewService(peer, replicaConfig.ServiceAddr))
	re.registerService(consts.DataServiceName, data.NewService(sc, ":"+strconv.Itoa(serverConfig.Port)))

	return re, nil
}

func (r *Region) registerService(name string, service iface.IService) {
	r.services[name] = service
}

func (r *Region) GetService(name string) iface.IService {
	return r.services[name]
}

func (r *Region) registerServices(services map[string]iface.IService) {
	for name, service := range services {
		r.registerService(name, service)
	}
}

func (r *Region) Start() {
	var wg sync.WaitGroup

	// 启动所有服务
	for _, svc := range r.services {
		wg.Add(1)
		go func(service iface.IService) {
			wg.Done()
			_ = service.Start()
		}(svc)
	}

	wg.Wait()

	klog.Info("all services start")
}
