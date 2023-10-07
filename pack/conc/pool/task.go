package conc

import (
	"context"
	"sync"
	"sync/atomic"
)

var taskPool sync.Pool

func init() {
	taskPool.New = newTask
}

type task struct {
	ctx  context.Context
	exec func()
	next *task
}

func (t *task) zero() {
	t.ctx = nil
	t.exec = nil
	t.next = nil
}

func (t *task) Recycle() {
	t.zero()
	taskPool.Put(t)
}

func newTask() any {
	return &task{}
}

type taskList struct {
	mutex sync.Mutex
	head  *task
	tail  *task
	count int32
}

// Push 插入新任务
func (list *taskList) Push(t *task) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.head == nil {
		list.head = t
		list.tail = t
	} else {
		list.tail.next = t
		list.tail = t
	}
	list.count++
}

// Pop 获取新任务
func (list *taskList) Pop() *task {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	if list.head == nil {
		return nil
	}

	list.count--
	t := list.head
	list.head = list.head.next
	return t
}

func (list *taskList) Count() int32 {
	return atomic.LoadInt32(&list.count)
}
