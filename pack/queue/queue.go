package queue

import (
	conc "TikBase/pack/conc/pool"
	"context"
	"sync"
	"time"
)

type Message struct {
	Topic string
	Data  []byte
}

type Pool interface {
	Publish(topic string, message Message)
	Subscribe(topic string, num int) <-chan Message
	Unsubscribe(topic string, ch <-chan Message)
}

type Handler func(message Message)

type MessageQueue struct {
	topics   map[string][]chan Message
	handlers map[string][]Handler
	mutex    sync.RWMutex
	pools    *conc.Pool // 线程池
}

func New() Pool {
	return &MessageQueue{
		topics:   make(map[string][]chan Message),
		handlers: make(map[string][]Handler),
		pools:    conc.NewPool("queue", 10),
	}
}

// Publish 向指定主题发布消息
func (q *MessageQueue) Publish(topic string, message Message) {
	q.mutex.RLock()
	subsChan, okChan := q.topics[topic]
	subsHandler, okHandler := q.handlers[topic]
	q.mutex.RUnlock()

	if okChan {
		go func(ch []chan Message) {
			for i := 0; i < len(ch); i++ {
				select {
				case subsChan[i] <- message:
				case <-time.After(time.Second):
				}
			}
		}(subsChan)
	}

	if okHandler {
		for i := 0; i < len(subsHandler); i++ {
			q.pools.Run(context.Background(), func() {
				subsHandler[i](message)
			})
		}
	}
}

func (q *MessageQueue) Subscribe(topic string, num int) <-chan Message {
	ch := make(chan Message, num)
	q.mutex.Lock()
	q.topics[topic] = append(q.topics[topic], ch)
	q.mutex.Unlock()
	return ch
}

func (q *MessageQueue) Unsubscribe(topic string, ch <-chan Message) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	subscribers, ok := q.topics[topic]
	if !ok {
		return
	}

	var newSubs []chan Message
	for _, subscriber := range subscribers {
		if subscriber == ch {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}

	q.topics[topic] = newSubs
}
