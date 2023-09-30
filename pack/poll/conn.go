package poll

import (
	"net"
	"time"
)

type Connection interface {
}

type connection struct {
	conn         net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxSize      int
}

func (c *connection) Reader() {

}
