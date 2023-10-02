package iface

import "context"

type Connection interface {
	Write([]byte) (int, error)
	Close() error
}

type Reply interface {
	ToBytes() []byte
}

type Handler interface {
	Handle(ctx context.Context, conn Connection)
	Close() error
}
