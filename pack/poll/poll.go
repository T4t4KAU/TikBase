package poll

import (
	"TikBase/iface"
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

func New(config *Config, handler iface.Handler) *NetPoll {
	return &NetPoll{
		reactor:  NewReactor(config.MaxConnect),
		closeCh:  make(chan struct{}),
		signalCh: make(chan os.Signal),
		address:  config.Address,
		handler:  handler,
	}
}

func (np *NetPoll) Run() error {
	signal.Notify(np.signalCh, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-np.signalCh
		switch sig {
		// 注册通知信号
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			np.closeCh <- struct{}{}
		}
	}()

	np.eventLoop()
	return nil
}

func (np *NetPoll) eventLoop() {
	lis, err := net.Listen("tcp", np.address)
	if err != nil {
		return
	}
	reactor := NewReactor(np.nconnect)
	reactor.Run(lis, np.closeCh, np.handler)
}
