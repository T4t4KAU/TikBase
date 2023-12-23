package tlog

import (
	"bytes"
	"fmt"
	"github.com/T4t4KAU/TikBase/pack/queue"
	"log"
	"os"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	Info("std log")
	SetOptions(WithLevel(DebugLevel))
	Debug("change std log to debug level")
	SetOptions(WithFormatter(&TextFormatter{IgnoreBasicFields: false}))

	// 输出到文件
	fd, err := os.OpenFile("test.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("create file test.log failed")
	}
	defer fd.Close()

	logger := New(WithLevel(InfoLevel),
		WithOutput(fd),
		WithFormatter(&TextFormatter{IgnoreBasicFields: false}),
	)
	logger.Info("custom log with json formatter")
}

func TestSendLog(t *testing.T) {
	var buff bytes.Buffer

	logger := New(WithLevel(InfoLevel),
		WithOutput(&buff),
		WithFormatter(&TextFormatter{IgnoreBasicFields: false}),
	)
	q := queue.New(queue.DefaultConfig)

	go func() {
		ch := q.Subscribe(func(msg queue.Message) bool {
			return msg.Topic == "test"
		})

		for msg := range ch {
			fmt.Print(msg.Data.(string))
		}
	}()

	for i := 0; i < 50; i++ {
		logger.Info("test log")
		q.Publish(queue.Message{
			Topic: "test",
			Data:  buff.String(),
		})
		buff.Reset()
		time.Sleep(time.Second)
	}
}
