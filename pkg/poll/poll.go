package poll

import (
	"errors"
	"github.com/T4t4KAU/TikBase/iface"
	"net"
	"os"
	"os/signal"
	"syscall"
)

type NetPoll struct {
	closeCh  chan struct{}
	signalCh chan os.Signal
	address  string
	handler  iface.Handler
	nconnect int32
	Name     string
}

func VerifyConfig(config Config) bool {
	// TODO: 验证有效性
	return true
}

func New(config Config, handler iface.Handler) (*NetPoll, error) {
	if !VerifyConfig(config) {
		return nil, errors.New("invalid config for poll")
	}

	return &NetPoll{
		closeCh:  make(chan struct{}),
		signalCh: make(chan os.Signal),
		address:  config.Address,
		handler:  handler,
		Name:     config.Name,
	}, nil
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
	reactor := NewReactor(p.nconnect, p.Name)
	reactor.handler = p.handler
	reactor.Run(lis, p.closeCh)
}
