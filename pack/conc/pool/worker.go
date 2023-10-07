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
				w.Recycle()
				return
			}
			t.exec()
			t.Recycle()
		}
	}()
}

func (w *worker) close() {
	atomic.AddInt32(&w.pool.nworker, -1)
}

func (w *worker) zero() {
	w.pool = nil
}

func (w *worker) Recycle() {
	w.zero()
	workerPool.Put(w)
}
