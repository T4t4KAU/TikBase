package logq

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/pkg/log"
	"github.com/T4t4KAU/TikBase/pkg/queue"
	"log"
	"os"
	"testing"
	"time"
)

func LogConsumer(ch queue.Subscriber) {
	for msg := range ch {
		buff, err := os.OpenFile("test.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)

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
	ch := New("test log", LogConsumer)

	logger := log.New(log.WithLevel(log.InfoLevel),
		log.WithOutput(ch),
		log.WithFormatter(&log.TextFormatter{IgnoreBasicFields: false}),
	)

	for i := 0; i < 50; i++ {
		logger.Info("test log")
		time.Sleep(time.Second)
	}

	time.Sleep(5 * time.Second)
}
