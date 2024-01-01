package iface

import "net"

type Connection interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	Close() error
}

type IConnection interface {
	Start() error
	Send(id int32, data []byte) error
	Close()
	Raw() *net.TCPConn
	ID() uint32
	RemoteAddr() net.Addr
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

type IMessage interface {
	GetDataLen() uint32
	GetId() uint32
	GetData() []byte

	SetMsgId() uint32
	SetData() []byte
	SetDataLen(uint32)
}

type IDataPack interface {
	GetHeadLen() uint32
	Pack(msg IMessage) ([]byte, error)
	Unpack([]byte) (IMessage, error)
}
