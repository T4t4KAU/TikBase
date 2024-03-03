package main

import (
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/proxy"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

var (
	ServerConfFile    = "./conf/server-config.yaml"
	BaseConfigFile    = "./conf/base-config.yaml"
	CacheConfigFile   = "./conf/cache-config.yaml"
	ReplicaConfigFile = "./conf/replica-config-master.yaml"
	SliceConfigFile   = "./conf/slice-config.yaml"
)

func start() {
	// 读取服务器配置文件
	serverConf, err := config.ReadServerConfigFile(ServerConfFile)
	if err != nil {
		panic(err)
	}

	var storeConf config.StoreConfig

	// 读取存储配置文件
	switch serverConf.EngineName {
	case "base":
		storeConf, err = config.ReadBaseConfigFile(BaseConfigFile)
	case "cache":
		storeConf, err = config.ReadCacheConfigFile(CacheConfigFile)
	default:
		panic("unknown engine name")
	}
	if err != nil {
		panic(err)
	}

	// 读取副本配置文件
	replicaConf, err := config.ReadReplicaConfigFile(ReplicaConfigFile)
	if err != nil {
		panic(err)
	}

	sliceConf, err := config.ReadSliceConfigFile(SliceConfigFile)
	if err != nil {
		panic(err)
	}

	print("listening at port:", serverConf.ListenPort)
	print("   using protocol:", serverConf.Protocol)
	println("   using engine:", serverConf.EngineName)

	// 启动代理
	err = proxy.Start(serverConf, storeConf, replicaConf, sliceConf)
	if err != nil {
		panic(err)
	}
}

func main() {
	println(logo)
	start()
}
