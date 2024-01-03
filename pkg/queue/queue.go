package queue

import (
	"context"
	conc "github.com/T4t4KAU/TikBase/pkg/conc/pool"
	"sync"
	"time"
)

type (
	Subscriber chan Message
	Filter     func(msg Message) bool
	Consumer   func(ch Subscriber)
)

type Message struct {
	Topic string // 主题
	Data  any
}

type Queue interface {
	Publish(message Message)
	Subscribe(topic string, size int) (Subscriber, bool)
	Evict(sub Subscriber)
}

type MessageQueue struct {
	mutex       sync.RWMutex
	timeout     time.Duration
	subscribers map[Subscriber]Filter
	capacity    int        // 队列长度
	workers     *conc.Pool // 超时时间
}

func New(config Config) *MessageQueue {
	return &MessageQueue{
		subscribers: make(map[Subscriber]Filter),
		workers:     conc.NewPool("message queue", config.nworker),
		capacity:    config.capacity,
		timeout:     config.timeout,
	}
}

// Subscribe 发起订阅
func (mq *MessageQueue) Subscribe(filter Filter) Subscriber {
	sub := make(Subscriber, mq.capacity)
	mq.mutex.Lock()
	mq.subscribers[sub] = filter
	mq.mutex.Unlock()
	return sub
}

// Evict 取消订阅
func (mq *MessageQueue) Evict(sub Subscriber) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	delete(mq.subscribers, sub)
	close(sub)
}

// Publish 发布
func (mq *MessageQueue) Publish(message Message) {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	for sub, f := range mq.subscribers {
		mq.workers.Run(context.Background(), func() {
			mq.send(sub, f, message)
		})
	}
}

func (mq *MessageQueue) send(sub Subscriber, filter Filter, message Message) {
	if filter != nil && !filter(message) {
		return
	}
	select {
	case sub <- message:
	case <-time.After(mq.timeout):
	}
}
