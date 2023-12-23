package main

import (
	"github.com/T4t4KAU/TikBase/pack/config"
	"github.com/T4t4KAU/TikBase/proxy"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

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

	err = proxy.Start(server, store)
	if err != nil {
		panic(err)
	}
}
