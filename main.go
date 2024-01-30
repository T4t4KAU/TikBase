package main

import (
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/proxy"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

func start() {
	serverConf, err := config.ReadServerConfigFile("./config/server-config.yaml")
	if err != nil {
		panic(err)
	}

	var storeConf config.StoreConfig

	switch serverConf.EngineName {
	case "base":
		storeConf, err = config.ReadBaseConfigFile("./config/base-config.yaml")
	case "cache":
		storeConf, err = config.ReadCacheConfigFile("./config/cache-config.yaml")
	default:
		panic("unknown engine name")
	}
	if err != nil {
		panic(err)
	}

	replicaConf, err := config.ReadReplicaConfigFile("./config/replica-config.yaml")
	if err != nil {
		panic(err)
	}

	print("listening at port:", serverConf.ListenPort)
	print("   using protocol:", serverConf.Protocol)
	println("   using engine:", serverConf.EngineName)

	err = proxy.Start(serverConf, storeConf, replicaConf)
	if err != nil {
		panic(err)
	}
}

func main() {
	println(logo)
	start()
}
