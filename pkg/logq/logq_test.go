package logq

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/pkg/queue"
	"github.com/T4t4KAU/TikBase/pkg/tlog"
	"log"
	"os"
	"testing"
	"time"
)

func LogConsumer(ch queue.Subscriber) {
	for msg := range ch {
		buff, err := os.OpenFile("test.tlog", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)

		if err != nil {
			log.Fatalln(err)
		}
		_, err = buff.Write(msg.Data.([]byte))
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Print(string(msg.Data.([]byte)))
	}
}

func TestLogQueue_Write(t *testing.T) {
	ch := New("test tlog", LogConsumer)

	logger := tlog.New(tlog.WithLevel(tlog.InfoLevel),
		tlog.WithOutput(ch),
		tlog.WithFormatter(&tlog.TextFormatter{IgnoreBasicFields: false}),
	)

	for i := 0; i < 50; i++ {
		logger.Info("test tlog")
		time.Sleep(time.Second)
	}

	time.Sleep(5 * time.Second)
}
