package proxy

import (
	"github.com/T4t4KAU/TikBase/cluster/region"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
)

func Start(server config.ServerConfig, store config.StoreConfig, replica config.ReplicaConfig, slices config.SliceConfig) (err error) {
	var eng iface.Engine

	switch server.EngineName {
	case "base":
		cfg := store.(config.BaseStoreConfig)
		eng, err = engine.NewBaseEngineWith(cfg)
	case "cache":
		cfg := store.(config.CacheStoreConfig)
		eng, err = engine.NewCacheEngineWith(cfg)
	}

	service, err := region.New(&replica, &slices, eng)
	if err != nil {
		panic(err)
	}

	go func() {
		// 启动所有服务
		service.Start()
	}()

	if replica.JoinAddr != "" {
		// TODO: 启动副本存储
	}

	select {}

	return nil
}
