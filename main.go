package main

import (
	"TikBase/engine"
	"TikBase/iface"
	"TikBase/pack/config"
	"TikBase/pack/net/http"
	"TikBase/pack/net/tiko"
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
	default:
		return nil, errors.New("invalid protocol")
	}
}

func startServer(server config.ServerConfig, store config.BaseStoreConfig) {
	eng, err := engine.NewEngine("base")

	go func() {
		err := http.StartServer(":9090", eng)
		if err != nil {
			panic(err)
		}
	}()

	handler, err := NewHandler("tiko", eng)
	if err != nil {
		panic(err)
	}

	p := poll.New(poll.Config{
		Address:    ":" + strconv.Itoa(server.RESPPort),
		MaxConnect: int32(server.WorkersNum),
		Timeout:    time.Second,
	}, handler)

	err = p.Run()
	if err != nil {
		panic(err)
	}
}

func main() {
	println(logo)
	server, err := config.ReadServerConfigFile("./config/server-config.yaml")
	if err != nil {
		panic(err)
	}
	store, err := config.ReadBaseConfigFile("./config/store-config.yaml")
	if err != nil {
		panic(err)
	}

	startServer(server, store)
}
