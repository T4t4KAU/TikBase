package cluster

import (
	"sync"
	"testing"
)

type config struct {
	mutex    sync.Mutex
	t        *testing.T
	finished int32
	n        int
}
