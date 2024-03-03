package proxy

import (
	"errors"
	"github.com/T4t4KAU/TikBase/cluster/region"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/net/resp"
	"github.com/T4t4KAU/TikBase/pkg/net/tiko"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"strconv"
	"time"
)

func NewHandler(name string, eng iface.Engine) (iface.Handler, error) {
	switch name {
	case "tiko":
		return tiko.NewHandler(eng), nil
	case "resp":
		return resp.NewHandler(eng), nil
	default:
		return nil, errors.New("invalid protocol")
	}
}

func Start(server config.ServerConfig, store config.StoreConfig, replica config.ReplicaConfig) (err error) {
	var eng iface.Engine

	switch server.EngineName {
	case "base":
		cfg := store.(config.BaseStoreConfig)
		eng, err = engine.NewBaseEngineWith(cfg)
	case "cache":
		cfg := store.(config.CacheStoreConfig)
		eng, err = engine.NewCacheEngineWith(cfg)
	}

	handler, err := NewHandler(server.Protocol, eng)
	if err != nil {
		panic(err)
	}

	service, err := region.New(&replica, eng)
	if err != nil {
		panic(err)
	}

	go func() {
		service.Start()
	}()

	if replica.JoinAddr != "" {
		// TODO: 启动副本存储
	}

	po, _ := poll.New(poll.Config{
		Address:    ":" + strconv.Itoa(server.ListenPort),
		MaxConnect: int32(server.WorkersNum),
		Timeout:    2 * time.Second,
	}, handler)

	err = po.Run()
	if err != nil {
		panic(err)
	}

	return nil
}
