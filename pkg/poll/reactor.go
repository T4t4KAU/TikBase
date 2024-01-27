package poll

import (
	"context"
	"github.com/T4t4KAU/TikBase/iface"
	conc "github.com/T4t4KAU/TikBase/pkg/conc/pool"
	"net"
)

type Reactor struct {
	workers *conc.Pool
	nworker int32
	errors  []error
	handler iface.Handler
}

func NewReactor(n int32, name string) *Reactor {
	return &Reactor{
		workers: conc.NewPool("subReactors", n),
		nworker: n,
	}
}

func (rec *Reactor) Run(lis net.Listener, ch chan struct{}) {
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
			rec.handler.Handle(conn)
		})
	}
}
