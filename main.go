package main

import (
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/proxy"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

func start() {

	server, err := config.ReadServerConfigFile("./config/server-config.yaml")
	if err != nil {
		panic(err)
	}

	var cfg config.StoreConfig

	switch server.EngineName {
	case "base":
		cfg, err = config.ReadBaseConfigFile("./config/base-config.yaml")
	case "cache":
		cfg, err = config.ReadCacheConfigFile("./config/cache-config.yaml")
	}
	if err != nil {
		panic(err)
	}

	print("listening at port:", server.ListenPort)
	print("   using protocol:", server.Protocol)
	println("   using engine:", server.EngineName)

	err = proxy.Start(server, cfg)
	if err != nil {
		panic(err)
	}
}

func main() {
	println(logo)
	start()
}
