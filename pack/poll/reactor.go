package poll

import (
	conc "TikBase/pack/conc/pool"
	"syscall"
)

type MainReactor struct {
	selector       syscall.FdSet
	subReactorPool *conc.Pool
	ioThreadNum    int32
}

func NewMainReactor(num int32) *MainReactor {
	return &MainReactor{
		subReactorPool: conc.NewPool("subReactors", num),
		ioThreadNum:    num,
	}
}

func (mr *MainReactor) Run() {

}

type SubReactor struct {
}
