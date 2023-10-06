package logq

import (
	"TikBase/iface"
	"TikBase/pack/queue"
	"sync"
	"time"
)

// LogQueue 日志队列
type LogQueue struct {
	channels map[string]*Channel
	mutex    sync.RWMutex
	*queue.MessageQueue
}

var (
	once   *LogQueue
	config = queue.NewConfig(time.Second*3, 30, 4)
)

// New 创建主题和对应通道
func New(name string, consumer queue.Consumer) iface.Channel {
	if once == nil {
		once = &LogQueue{
			MessageQueue: queue.New(config),
			channels:     make(map[string]*Channel),
		}
	}

	// 查找指定channel
	once.mutex.RLock()
	ch, ok := once.channels[name]
	once.mutex.RUnlock()

	if ok {
		return ch
	}

	// 不存在则创建channel
	ch = &Channel{
		Name:     name,
		mq:       once,
		consumer: consumer,
	}

	// 订阅指定topic
	ch.sub = once.Subscribe(func(msg *queue.Message) bool {
		return msg.Topic == name
	})

	// 增加通道引用计数
	ch.IncCount()

	once.mutex.Lock()
	// 注册通道
	once.channels[name] = ch
	once.mutex.Unlock()

	go ch.Consume()

	return ch
}

func (q *LogQueue) Remove(name string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	delete(q.channels, name)
}
