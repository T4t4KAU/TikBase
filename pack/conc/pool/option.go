package conc

import (
	"errors"
	"sync"
)

var pools sync.Map

func RegisterPool(p *Pool) error {
	_, loaded := pools.LoadOrStore(p.Name(), p)
	if loaded {
		return errors.New(p.Name() + "already registered")
	}
	return nil
}

func GetPool(name string) *Pool {
	p, ok := pools.Load(name)
	if !ok {
		return nil
	}
	return p.(*Pool)
}
