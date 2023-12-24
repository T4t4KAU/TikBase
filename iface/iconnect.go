package iface

type Connection interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	Close() error
}

type Reply interface {
	ToBytes() []byte
}

type Handler interface {
	Handle(conn Connection)
}

type Client interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Del(key string) error
	Expire(key string, ttl int64) error
}
