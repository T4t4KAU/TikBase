package resp

import (
	"TikCache/pack/wait"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn    net.Conn
	waiting wait.Wait
	mutex   sync.Mutex
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// 发送数据
func (c *Connection) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	c.mutex.Lock()
	defer func() {
		c.waiting.Done()
		c.mutex.Unlock()
	}()
	c.waiting.Add(1)
	_, err := c.conn.Write(bytes)
	return err
}

func (c *Connection) Close() {
	c.waiting.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
