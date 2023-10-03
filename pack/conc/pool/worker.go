package conc

import (
	"sync"
	"sync/atomic"
)

var workerPool sync.Pool

func init() {
	workerPool.New = newWorker
}

type worker struct {
	pool *Pool
}

func newWorker() any {
	return &worker{}
}

func (w *worker) work() {
	go func() {
		for {
			t := w.pool.tasks.Pop()
			if t == nil {
				w.close()
				return
			}
			t.exec()
		}
	}()
}

func (w *worker) close() {
	atomic.AddInt32(&w.pool.nworker, -1)
}
