package main

import (
	"TikBase/engine"
	"TikBase/iface"
	"TikBase/pack/config"
	"TikBase/pack/net/http"
	"TikBase/pack/net/tcp/resp"
	"TikBase/pack/net/tcp/tiko"
	"TikBase/pack/poll"
	"errors"
	"strconv"
	"time"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

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

func startServer(config config.Config) {
	eng, err := engine.NewEngine(config.Type)
	if err != nil {
		panic(err)
	}

	if config.Protocol == "http" {
		eng, err = engine.NewCacheEngine()
		if err != nil {
			panic(err)
		}
		s := http.NewServer(eng)
		err = s.Run(":9096")
		if err != nil {
			panic(err)
		}
	}

	handler, err := NewHandler(config.Protocol, eng)
	if err != nil {
		panic(err)
	}

	p := poll.New(poll.Config{
		Address:    ":" + strconv.Itoa(config.TcpPort),
		MaxConnect: int32(config.MaxConn),
		Timeout:    time.Second,
	}, handler)

	err = p.Run()
	if err != nil {
		panic(err)
	}
}

func main() {
	println(logo)
	c, err := config.ReadConfigFile("config.yaml")
	if err != nil {
		panic(err)
	}

	println("using protocol:", c.Protocol, "\nusing engine:", c.Type, "\nstart server at", c.Host)
	startServer(*c)
}
