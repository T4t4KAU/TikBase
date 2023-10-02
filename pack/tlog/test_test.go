package tlog

import (
	"log"
	"os"
	"testing"
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

	l := New(WithLevel(InfoLevel),
		WithOutput(fd),
		WithFormatter(&TextFormatter{IgnoreBasicFields: false}),
	)
	l.Info("custom log with json formatter")
}
