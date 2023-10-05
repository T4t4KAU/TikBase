package main

import (
	"TikBase/engine"
	"TikBase/iface"
	"TikBase/pack/net/tcp/resp"
	"TikBase/pack/net/tcp/tiko"
	"TikBase/pack/poll"
	"errors"
	"flag"
	"time"
)

var proto, name *string

func init() {
	// 定义命令行参数
	proto = flag.String("proto", "tiko", "Protocol")
	name = flag.String("name", "cache", "Engine")

	// 解析命令行参数
	flag.Parse()

	if proto == nil {
		*proto = "tiko"
	}
	if name == nil {
		*name = "cache"
	}
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

func startServer() {
	eng, err := engine.NewEngine(*name)
	if err != nil {
		panic(err)
	}
	handler, err := NewHandler(*proto, eng)
	if err != nil {
		panic(err)
	}

	p := poll.New(&poll.Config{
		Address:    "127.0.0.1:9999",
		MaxConnect: 20,
		Timeout:    time.Second,
	}, handler)

	err = p.Run()
	if err != nil {
		panic(err)
	}
}

func main() {
	startServer()
}
