package queue

import (
	"testing"
	"time"
)

func TestMessageQueue_Publish(t *testing.T) {
	q := New(&Config{
		timeout:  time.Second,
		capacity: 10,
		nworker:  5,
	})

	ch := q.Subscribe(func(msg *Message) bool {
		return msg.Topic == "test"
	})

	go func() {
		for msg := range ch {
			println(msg.Data.(int))
		}
	}()

	for i := 1; i <= 50; i++ {
		ch <- &Message{
			Topic: "test",
			Data:  i,
		}
	}

	time.Sleep(3 * time.Second)
}

func TestMessageQueue_Subscribe(t *testing.T) {
	q := New(&Config{
		timeout:  time.Second,
		capacity: 10,
		nworker:  5,
	})

	ch := q.Subscribe(func(msg *Message) bool {
		return msg.Topic == "test"
	})

	go func() {
		for msg := range ch {
			println(msg.Data.(string))
		}
	}()

	for i := 1; i <= 50; i++ {
		q.Publish(&Message{
			Topic: "test",
			Data:  time.Now().Format("2006-01-02 15:04:05"),
		})
		time.Sleep(time.Second)
	}

	time.Sleep(60 * time.Second)
}
