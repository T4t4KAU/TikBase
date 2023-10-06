package engine

import (
	"TikBase/engine/caches"
	"TikBase/engine/values"
	"TikBase/iface"
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

func (eng *CacheEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	switch ins {
	case iface.SET_STR:
		return eng.ExecSetString(args)
	case iface.GET_STR:
		return eng.ExecGetString(args)
	case iface.DEL:
		return eng.ExecDelKey(args)
	case iface.EXPIRE:
		return eng.ExecExpire(args)
	default:
		return NewUnknownCacheResult()
	}
}

func (eng *CacheEngine) ExecSetString(args [][]byte) *CacheResult {
	key := string(args[0])
	val := parseSetStringArgs(args)
	ok := eng.SetString(key, val, values.NeverExpire)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errExceedCapacity,
		}
	}
	return NewSuccCacheResult()
}

func (eng *CacheEngine) ExecGetString(args [][]byte) *CacheResult {
	key := string(args[0])
	val, ok := eng.Get(key)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errKeyNotFound,
		}
	}
	return &CacheResult{
		succ: true,
		data: [][]byte{val.Bytes()},
	}
}

func (eng *CacheEngine) ExecDelKey(args [][]byte) *CacheResult {
	key := string(args[0])
	ok := eng.Del(key)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errKeyNotFound,
		}
	}
	return &CacheResult{
		succ: true,
	}
}

func (eng *CacheEngine) ExecExpire(args [][]byte) *CacheResult {
	key := string(args[0])
	ttl := parseExpireKeyArgs(args)
	ok := eng.Expire(key, ttl)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errKeyNotFound,
		}
	}
	return &CacheResult{
		succ: true,
	}
}

func (eng *CacheEngine) ExecKeys() *CacheResult {
	return &CacheResult{
		succ: true,
		data: eng.Keys(),
	}
}
