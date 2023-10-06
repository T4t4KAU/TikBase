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

func NewReactor(n int32) *Reactor {
	return &Reactor{
		workers: conc.NewPool("subReactors", n),
		nworker: n,
	}
}

func (rec *Reactor) Run(lis net.Listener, ch chan struct{}, handler iface.Handler) {
	go func() {
		<-ch
		_ = lis.Close()
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
