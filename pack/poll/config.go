package poll

import (
	"time"
)

type Config struct {
	Address    string
	MaxConnect uint32
	Timeout    time.Duration
}

func NewConfig(addr string, n uint32, timeout time.Duration) *Config {
	return &Config{
		Address:    addr,
		MaxConnect: n,
		Timeout:    timeout,
	}
}
