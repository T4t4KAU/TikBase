package poll

import (
	"TikBase/pack/iface"
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Event interface {
}

type FileEvent struct {
}

type TimeEvent struct {
}

type OnPrepare func(conn iface.Connection) context.Context

type OnConnect func(ctx context.Context, conn iface.Connection)

type OnRequest func(ctx context.Context, connection iface.Connection) error

type EventLoop struct {
	size int
	stop bool
}

func NewEventLoop(size int) *EventLoop {
	return &EventLoop{
		size: size,
		stop: false,
	}
}

// Run 启动事件循环
func (e *EventLoop) Run(config Config, handler iface.Handler) error {
	closeCh := make(chan struct{})
	sigCh := make(chan os.Signal)

	// 注册通知信号
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		// 注册通知信号
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeCh <- struct{}{}
		}
	}()
	lis, err := net.Listen("tcp", config.Address)
	if err != nil {
		return err
	}
	e.Serve(lis, handler, closeCh)
	return nil
}

// Serve 处理连接
func (e *EventLoop) Serve(listener net.Listener, handler iface.Handler, closeCh <-chan struct{}) {
	go func() {
		<-closeCh
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	wg.Wait()
}
