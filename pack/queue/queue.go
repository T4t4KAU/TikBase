package queue

import (
	conc "TikBase/pack/conc/pool"
	"context"
	"sync"
	"time"
)

type (
	Subscriber chan *Message
	Filter     func(msg *Message) bool
	Consumer   func(ch Subscriber)
)

type Message struct {
	Topic string
	Data  any
}

type Queue interface {
	Publish(message *Message)
	Subscribe(topic string, size int) (Subscriber, bool)
	Evict(sub Subscriber)
}

type MessageQueue struct {
	topics      map[string]Subscriber
	mutex       sync.RWMutex
	timeout     time.Duration
	subscribers map[Subscriber]Filter
	capacity    int
	workers     *conc.Pool
}

func New(config *Config) *MessageQueue {
	return &MessageQueue{
		topics:      make(map[string]Subscriber),
		subscribers: make(map[Subscriber]Filter),
		workers:     conc.NewPool("queue", config.nworker),
		capacity:    config.capacity,
		timeout:     config.timeout,
	}
}

func (mq *MessageQueue) Subscribe(filter Filter) Subscriber {
	sub := make(Subscriber, mq.capacity)
	mq.mutex.Lock()
	mq.subscribers[sub] = filter
	mq.mutex.Unlock()
	return sub
}

func (mq *MessageQueue) Evict(sub Subscriber) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	delete(mq.subscribers, sub)
	close(sub)
}

func (mq *MessageQueue) Publish(message *Message) {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	for sub, f := range mq.subscribers {
		mq.workers.Run(context.Background(), func() {
			mq.send(sub, f, message)
		})
	}
}

func (mq *MessageQueue) send(sub Subscriber, filter Filter, message *Message) {
	if filter != nil && !filter(message) {
		return
	}
	select {
	case sub <- message:
	case <-time.After(mq.timeout):
	}
}
