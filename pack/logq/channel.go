package logq

import (
	"TikBase/pack/queue"
	"errors"
	"sync/atomic"
)

type Channel struct {
	Name     string           // 名称
	count    int32            // 引用计数
	mq       *LogQueue        // 指向日志队列
	sub      queue.Subscriber // 订阅通道
	consumer queue.Consumer
	closed   chan struct{}
}

// 写入日志
func (ch *Channel) Write(data []byte) (int, error) {
	if !ch.Available() {
		return 0, errors.New("channel already closed")
	}
	ch.mq.Publish(&queue.Message{
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
	atomic.AddInt32(&ch.count, -1)
	if !ch.Available() {
		ch.mq.Remove(ch.Name)
	}
}

func (ch *Channel) Available() bool {
	return atomic.LoadInt32(&ch.count) > 0
}

func (ch *Channel) IncCount() {
	atomic.AddInt32(&ch.count, 1)
}
