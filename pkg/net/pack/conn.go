package pack

import (
	"errors"
	"io"
	"net"
)

type Connection struct {
	conn   *net.TCPConn
	Id     uint32
	closed bool
	exit   chan struct{}
}

func (c *Connection) StartReader() error {
	defer c.Close()

	for {
		pack := NewDataPack()
		head := make([]byte, pack.GetHeadLen())
		if _, err := io.ReadFull(c.Raw(), head); err != nil {
			c.exit <- struct{}{}
		}
	}
}

func (c *Connection) Start() {
	go func() {
		_ = c.StartReader()
	}()

	for {
		select {
		case <-c.exit:
			return
		}
	}
}

func (c *Connection) Send(id uint32, data []byte) error {
	if c.closed == true {
		return errors.New("connection closed")
	}
	pa := NewDataPack()
	msg, err := pa.Pack(NewMsgPackage(id, data))
	if err != nil {
		return errors.New("pack error msg")
	}

	if _, err = c.conn.Write(msg); err != nil {
		c.exit <- struct{}{}
		return errors.New("conn write error")
	}

	return nil
}

func (c *Connection) Close() {
	if c.closed == true {
		return
	}
	c.closed = true

	_ = c.conn.Close()
	c.exit <- struct{}{}
	close(c.exit)
}

func (c *Connection) Raw() *net.TCPConn {
	return c.conn
}

func (c *Connection) ID() uint32 {
	return c.Id
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func NewConnection(conn *net.TCPConn, connId uint32) *Connection {
	return &Connection{
		conn:   conn,
		Id:     connId,
		closed: false,
		exit:   make(chan struct{}, 1),
	}
}
