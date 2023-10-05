package queue

import "time"

type Config struct {
	timeout  time.Duration
	capacity int
	nworker  int32
}

var DefaultConfig = &Config{
	timeout:  time.Second,
	capacity: 10,
	nworker:  5,
}

func NewConfig(timeout time.Duration, capacity int, nworker int32) *Config {
	return &Config{
		timeout:  timeout,
		capacity: capacity,
		nworker:  nworker,
	}
}
