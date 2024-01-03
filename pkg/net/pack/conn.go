package pack

import (
	"io"
	"net"
)

type Connection struct {
	conn   *net.TCPConn
	Id     uint32
	closed bool
	exit   chan struct{}
}

func (c *Connection) Start() error {
	defer c.Close()

	for {
		pack := NewDataPack()
		head := make([]byte, pack.GetHeadLen())
		if _, err := io.ReadFull(c.Raw(), head); err != nil {
			c.exit <- struct{}{}
		}
	}
}

func (c *Connection) Send(id int32, data []byte) error {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Close() {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Raw() *net.TCPConn {
	return c.conn
}

func (c *Connection) ID() uint32 {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) RemoteAddr() net.Addr {
	//TODO implement me
	panic("implement me")
}

func NewConnection(conn *net.TCPConn, connId uint32) *Connection {
	return &Connection{
		conn:   conn,
		Id:     connId,
		closed: false,
		exit:   make(chan struct{}, 1),
	}
}
