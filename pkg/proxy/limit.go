package proxy

import (
	"sync"
	"time"
)

const (
	LimitRate = 10
	Capacity  = 100
)

// Limiter 定义限流器
type Limiter struct {
	*TokenBucket
}

func NewLimiter() *Limiter {
	return &Limiter{
		TokenBucket: NewTokenBucket(LimitRate, Capacity),
	}
}

type TokenBucket struct {
	rate     float64
	capacity int
	tokens   int
	utime    time.Time
	mutex    sync.Mutex
}

// NewTokenBucket 令牌桶实现
func NewTokenBucket(rate float64, capacity int) *TokenBucket {
	return &TokenBucket{
		rate:     rate,
		capacity: capacity,
		tokens:   capacity,
	}
}

func (b *TokenBucket) Allow() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.utime).Seconds()
	b.utime = now

	b.tokens += int(elapsed * b.rate)
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
