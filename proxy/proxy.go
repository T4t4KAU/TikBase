package proxy

import (
	"github.com/T4t4KAU/TikBase/cluster/region"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
)

func Start(server config.ServerConfig, store config.StoreConfig, replica config.ReplicaConfig) (err error) {
	var eng iface.Engine

	// 初始化存储引擎
	switch server.EngineName {
	case "base":
		cfg := store.(config.BaseStoreConfig)
		eng, err = engine.NewBaseEngineWith(cfg)
	case "cache":
		cfg := store.(config.CacheStoreConfig)
		eng, err = engine.NewCacheEngineWith(cfg)
	}

	// 启动region服务
	service, err := region.New(&replica, &server, eng)
	if err != nil {
		panic(err)
	}

	go func() {
		// 启动所有服务
		service.Start()
	}()

	select {}

	return nil
}
