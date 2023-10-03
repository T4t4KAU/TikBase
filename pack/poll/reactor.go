package poll

import (
	"TikBase/iface"
	conc "TikBase/pack/conc/pool"
	"context"
	"net"
)

type Reactor struct {
	threadPool  *conc.Pool
	ioThreadNum int32
	errors      []error
}

func NewReactor(num int32) *Reactor {
	return &Reactor{
		threadPool:  conc.NewPool("subReactors", num),
		ioThreadNum: num,
	}
}

func (mr *Reactor) Run(lis net.Listener, ch chan struct{}, handler iface.Handler) {
	go func() {
		<-ch
		_ = lis.Close()
		_ = handler.Close()
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		mr.threadPool.Run(context.Background(), func() {
			handler.Handle(conn)
		})
	}
}
