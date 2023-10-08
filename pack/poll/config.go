package poll

import (
	"time"
)

type Config struct {
	Address    string
	MaxConnect int32
	Timeout    time.Duration
}

func NewConfig(addr string, num int32, timeout time.Duration) Config {
	return Config{
		Address:    addr,
		MaxConnect: num,
		Timeout:    timeout,
	}
}
