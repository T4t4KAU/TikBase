package engine

import (
	"TikBase/engine/caches"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/errno"
	"errors"
)

type CacheEngine struct {
	*caches.Cache
	execFunc map[iface.INS]ExecFunc
}

func NewCacheEngine() (*CacheEngine, error) {
	c, err := caches.New()
	if err != nil {
		return nil, err
	}

	eng := &CacheEngine{
		Cache: c,
	}

	eng.initExecFunc()
	return eng, nil
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
	if fn, ok := eng.execFunc[ins]; ok {
		return fn(args)
	}
	return NewUnknownCacheResult()
}

func (eng *CacheEngine) registerExecFunc(ins iface.INS, fn ExecFunc) {
	eng.execFunc[ins] = fn
}

func (eng *CacheEngine) initExecFunc() {
	eng.registerExecFunc(iface.GET_STR, eng.ExecStrGet)
	eng.registerExecFunc(iface.SET_STR, eng.ExecStrSet)
	eng.registerExecFunc(iface.DEL, eng.ExecDelKey)
}

func (eng *CacheEngine) ExecStrSet(args [][]byte) iface.Result {
	key := string(args[0])
	val, err := parseSetStringArgs(args)
	if err != nil {
		return &CacheResult{
			succ: false,
			err:  err,
		}
	}
	ok := eng.SetString(key, val, values.NeverExpire)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errno.ErrExceedCapacity,
		}
	}
	return NewSuccCacheResult()
}

func (eng *CacheEngine) ExecStrGet(args [][]byte) iface.Result {
	key := string(args[0])
	val, ok := eng.Get(key)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errno.ErrKeyNotFound,
		}
	}
	return &CacheResult{
		succ: true,
		data: [][]byte{val.Bytes()},
	}
}

func (eng *CacheEngine) ExecDelKey(args [][]byte) iface.Result {
	key := string(args[0])
	ok := eng.Del(key)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errno.ErrKeyNotFound,
		}
	}
	return &CacheResult{
		succ: true,
	}
}

func (eng *CacheEngine) ExecExpire(args [][]byte) iface.Result {
	key := string(args[0])
	ttl, err := parseExpireKeyArgs(args)
	if err != nil {
		return &CacheResult{
			succ: false,
			err:  err,
		}
	}
	ok := eng.Expire(key, ttl)
	if !ok {
		return &CacheResult{
			succ: false,
			err:  errno.ErrKeyNotFound,
		}
	}
	return &CacheResult{
		succ: true,
	}
}

func (eng *CacheEngine) ExecKeys() iface.Result {
	return &CacheResult{
		succ: true,
		data: eng.Keys(),
	}
}
