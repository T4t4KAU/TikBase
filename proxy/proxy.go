package proxy

import (
	"errors"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pack/config"
	"github.com/T4t4KAU/TikBase/pack/net/http"
	"github.com/T4t4KAU/TikBase/pack/net/tiko"
	"github.com/T4t4KAU/TikBase/pack/poll"
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
	default:
		return nil, errors.New("invalid protocol")
	}
}

func Start(server config.ServerConfig, store any) (err error) {
	var eng iface.Engine

	switch server.EngineName {
	case "base":
		cfg := store.(config.BaseStoreConfig)
		eng, err = engine.NewBaseEngineWith(cfg)
	case "cache":
		eng, err = engine.NewCacheEngine()
	}

	handler, err := NewHandler(server.Protocol, eng)
	if err != nil {
		panic(err)
	}

	p := poll.New(poll.Config{
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
