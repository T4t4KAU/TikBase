package poll

import (
	"time"
)

type Config struct {
	Address    string
	MaxConnect uint32
	Timeout    time.Duration
}

func NewConfig(addr string, num uint32, timeout time.Duration) *Config {
	return &Config{
		Address:    addr,
		MaxConnect: num,
		Timeout:    timeout,
	}
}
