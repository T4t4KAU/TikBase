package iface

type Connection interface {
	Write([]byte) (int, error)
	Close() error
}

type Reply interface {
	ToBytes() []byte
}

type Handler interface {
	Handle(conn Connection)
	Close() error
}
