package conc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPool(t *testing.T) {
	var n int32
	p := NewPool("test", 100)
	var wg sync.WaitGroup

	for i := 0; i < 3000; i++ {
		wg.Add(1)
		p.Run(context.Background(), func() {
			defer wg.Done()
			atomic.AddInt32(&n, 1)
		})
	}

	wg.Wait()
	println(n)
}
