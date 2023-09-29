package engine

import (
	"TikCache/engine/caches"
	"TikCache/engine/values"
	"TikCache/pack/iface"
	"errors"
)

type CacheEngine struct {
	*caches.Cache
}

func NewCacheEngine() *CacheEngine {
	return &CacheEngine{
		caches.New(),
	}
}

type CacheResult struct {
	succ bool
	data [][]byte
	err  error
}

func (r *CacheResult) Success() bool {
	return r.succ
}

func (r *CacheResult) Error() error {
	return r.err
}

func (r *CacheResult) Status() int {
	return 0
}

func (r *CacheResult) Data() [][]byte {
	return r.data
}

func NewSuccCacheResult() *CacheResult {
	return &CacheResult{
		succ: true,
	}
}

func NewUnknownCacheResult() *CacheResult {
	return &CacheResult{
		succ: false,
		err:  errors.New("unknown instruction type"),
	}
}

func (c *CacheEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	switch ins {
	case iface.SET_STR:
		return c.ExecSetString(args)
	case iface.GET_STR:
		return c.ExecGetString(args)
	default:
		return NewUnknownCacheResult()
	}
}

func (c *CacheEngine) ExecSetString(args [][]byte) *CacheResult {
	val := parseSetStringArgs(args)
	key := string(args[0])
	c.SetString(key, val, values.NeverExpire)
	return NewSuccCacheResult()
}

func (c *CacheEngine) ExecGetString(args [][]byte) *CacheResult {
	key := string(args[0])
	val, ok := c.Get(key)
	if !ok {
		return &CacheResult{
			succ: false,
		}
	}
	return &CacheResult{
		succ: true,
		data: [][]byte{val.Bytes()},
	}
}
