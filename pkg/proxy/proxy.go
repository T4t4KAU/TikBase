package proxy

import (
	"errors"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/net/http"
	"github.com/T4t4KAU/TikBase/pkg/net/resp"
	"github.com/T4t4KAU/TikBase/pkg/net/tiko"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"strconv"
	"time"
)

type Proxy struct {
	limiter *Limiter
	eng     iface.Engine
	reactor *poll.NetPoll
}

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

func Start(server config.ServerConfig, store config.StoreConfig) (err error) {
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

	p, _ := poll.New(poll.Config{
		Address:    ":" + strconv.Itoa(server.ListenPort),
		MaxConnect: int32(server.WorkersNum),
		Timeout:    time.Second,
	}, handler)

	err = p.Run()
	if err != nil {
		panic(err)
	}

	go func() {
		err := http.StartServer(":9090", eng)
		if err != nil {
			panic(err)
		}
	}()

	return nil
}
