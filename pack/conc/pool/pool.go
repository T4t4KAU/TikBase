package conc

import (
	"context"
	"sync/atomic"
)

// Pool 任务池
type Pool struct {
	name     string
	capacity int32
	tasks    *taskList
	ntask    int32
	nworker  int32
}

func NewPool(name string, capacity int32) *Pool {
	pool := &Pool{
		name:     name,
		capacity: capacity,
		tasks:    &taskList{},
	}

	return pool
}

func (p *Pool) Name() string {
	return p.name
}

func (p *Pool) Run(ctx context.Context, fn func()) {
	// 获取任务
	t := taskPool.Get().(*task)
	t.ctx = ctx
	t.exec = fn

	p.tasks.Push(t)
	if p.check() {
		atomic.AddInt32(&p.nworker, 1)
		// 创建工作程序
		w := workerPool.Get().(*worker)
		w.pool = p
		w.work()
	}
}

func (p *Pool) WorkerCount() int32 {
	return atomic.LoadInt32(&p.nworker)
}

func (p *Pool) Capacity() int32 {
	return atomic.LoadInt32(&p.capacity)
}

func (p *Pool) check() bool {
	return p.WorkerCount() < p.Capacity() || p.WorkerCount() == 0
}
