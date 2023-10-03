package resp

import (
	"TikBase/pack/wait"
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
func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	c.mutex.Lock()
	defer func() {
		c.waiting.Done()
		c.mutex.Unlock()
	}()
	c.waiting.Add(1)
	return c.conn.Write(bytes)
}

func (c *Connection) Read(bytes []byte) (int, error) {
	c.mutex.Lock()
	defer func() {
		c.waiting.Done()
		c.mutex.Unlock()
	}()
	c.waiting.Add(1)
	return c.conn.Write(bytes)
}

func (c *Connection) Close() error {
	c.waiting.WaitWithTimeout(10 * time.Second)
	return c.conn.Close()
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
