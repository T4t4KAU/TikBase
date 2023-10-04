package poll

import (
	"TikBase/iface"
	conc "TikBase/pack/conc/pool"
	"context"
	"net"
)

type Reactor struct {
	workers *conc.Pool
	nworker int32
	errors  []error
}

func NewReactor(num int32) *Reactor {
	return &Reactor{
		workers: conc.NewPool("subReactors", num),
		nworker: num,
	}
}

func (rec *Reactor) Run(lis net.Listener, ch chan struct{}, handler iface.Handler) {
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
		rec.workers.Run(context.Background(), func() {
			handler.Handle(conn)
		})
	}
}
