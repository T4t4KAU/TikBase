package poll

import (
	"github.com/T4t4KAU/TikBase/iface"
	"net"
	"os"
	"os/signal"
	"syscall"
)

type NetPoll struct {
	reactor  *Reactor
	closeCh  chan struct{}
	signalCh chan os.Signal
	address  string
	handler  iface.Handler
	nconnect int32
}

func New(config Config, handler iface.Handler) *NetPoll {
	return &NetPoll{
		reactor:  NewReactor(config.MaxConnect),
		closeCh:  make(chan struct{}),
		signalCh: make(chan os.Signal),
		address:  config.Address,
		handler:  handler,
	}
}

func (p *NetPoll) Run() error {
	signal.Notify(p.signalCh, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-p.signalCh
		switch sig {
		// 注册通知信号
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			p.closeCh <- struct{}{}
		}
	}()

	p.EventLoop()

	return nil
}

func (p *NetPoll) EventLoop() {
	lis, err := net.Listen("tcp", p.address)
	if err != nil {
		return
	}
	reactor := NewReactor(p.nconnect)
	reactor.handler = p.handler
	reactor.Run(lis, p.closeCh)
}
