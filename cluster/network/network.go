package network

import "sync"

type Network struct {
	mutex    sync.Mutex
	reliable byte
}
