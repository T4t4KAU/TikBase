package main

import (
	"TikBase/engine"
	"TikBase/iface"
	"TikBase/pack/net/http"
	"TikBase/pack/net/tcp/resp"
	"TikBase/pack/net/tcp/tiko"
	"TikBase/pack/poll"
	"errors"
	"flag"
	"time"
)

var proto, name *string
var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

const address = "127.0.0.1:9999"

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

func StartServer(name, proto string) {
	eng, err := engine.NewEngine(name)
	if err != nil {
		panic(err)
	}

	if proto == "http" {
		s := http.NewServer(engine.NewCacheEngine())
		err = s.Run(":9999")
		if err != nil {
			panic(err)
		}
	}

	handler, err := NewHandler(proto, eng)
	if err != nil {
		panic(err)
	}

	p := poll.New(poll.Config{
		Address:    address,
		MaxConnect: 20,
		Timeout:    time.Second,
	}, handler)

	err = p.Run()
	if err != nil {
		panic(err)
	}
}

func main() {
	println(logo)
	println("using protocol:", *proto, "\nusing engine:", *name, "\nstart server at", address)
	StartServer(*name, *proto)
}
