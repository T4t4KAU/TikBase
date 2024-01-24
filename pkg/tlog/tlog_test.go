package tlog

import (
	"bytes"
	"fmt"
	"github.com/T4t4KAU/TikBase/pkg/queue"
	"log"
	"os"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	Info("std tlog")
	SetOptions(WithLevel(DebugLevel))
	Debug("change std tlog to debug level")
	SetOptions(WithFormatter(&TextFormatter{IgnoreBasicFields: false}))

	// 输出到文件
	fd, err := os.OpenFile("test.tlog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("create file test.tlog failed")
	}
	defer func() {
		_ = fd.Close()
	}()

	logger := New(WithLevel(InfoLevel),
		WithOutput(fd),
		WithFormatter(&TextFormatter{IgnoreBasicFields: false}),
	)
	logger.Info("custom tlog with json formatter")
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
		logger.Info("test tlog")
		q.Publish(queue.Message{
			Topic: "test",
			Data:  buff.String(),
		})
		buff.Reset()
		time.Sleep(time.Second)
	}
}
