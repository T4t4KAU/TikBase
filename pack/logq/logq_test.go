package logq

import (
	"TikBase/pack/queue"
	"TikBase/pack/tlog"
	"fmt"
	"testing"
	"time"
)

func LogConsumer(ch queue.Subscriber) {
	for msg := range ch {
		fmt.Print(string(msg.Data.([]byte)))
	}
}

func TestLogQueue_Write(t *testing.T) {
	ch := New("test log", LogConsumer)

	logger := tlog.New(tlog.WithLevel(tlog.InfoLevel),
		tlog.WithOutput(ch),
		tlog.WithFormatter(&tlog.TextFormatter{IgnoreBasicFields: false}),
	)

	for i := 0; i < 50; i++ {
		logger.Info("test log")
		time.Sleep(time.Second)
	}

	time.Sleep(5 * time.Second)
}
