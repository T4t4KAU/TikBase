package logq

import (
	"TikBase/pack/queue"
	"errors"
	"sync"
	"sync/atomic"
)

var channelPool sync.Pool

func init() {
	channelPool.New = newChannel
}

type Channel struct {
	Name     string           // 名称
	count    int32            // 引用计数
	mq       *LogQueue        // 指向日志队列
	sub      queue.Subscriber // 订阅通道
	consumer queue.Consumer
	closed   chan struct{}
}

func newChannel() any {
	return &Channel{}
}

// 写入日志
func (ch *Channel) Write(data []byte) (int, error) {
	if !ch.Available() {
		return 0, errors.New("channel already closed")
	}
	ch.mq.Publish(queue.Message{
		Topic: ch.Name,
		Data:  data,
	})
	return len(data), nil
}

func (ch *Channel) Consume() {
	if !ch.Available() {
		return
	}
	ch.consumer(ch.sub)
}

func (ch *Channel) Close() {
	if !ch.Available() {
		return
	}
	ch.mq.Evict(ch.sub)
	ch.DecCount()
	if !ch.Available() {
		ch.mq.Remove(ch.Name)
	}
	ch.Recycle()
}

func (ch *Channel) Available() bool {
	return atomic.LoadInt32(&ch.count) > 0
}

func (ch *Channel) IncCount() {
	atomic.AddInt32(&ch.count, 1)
}

func (ch *Channel) DecCount() {
	atomic.AddInt32(&ch.count, -1)
}

func (ch *Channel) Recycle() {
	ch.mq = nil
	ch.sub = nil
	ch.consumer = nil
	ch.closed = nil
	ch.Name = ""
	channelPool.Put(ch)
}
