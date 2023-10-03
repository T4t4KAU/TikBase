package poll

import "net"

type server struct {
	channel *net.TCPListener
}
