package engine

import (
	"TikBase/engine/caches"
	"TikBase/engine/values"
	"TikBase/iface"
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
	data []byte
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

func (r *CacheResult) Data() []byte {
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

func NewCacheErrorResult(err error) *CacheResult {
	if err != nil {
		return &CacheResult{
			succ: false,
			err:  err,
		}
	}
	return NewSuccCacheResult()
}

func NewCacheResult(succ bool, data []byte, err error) *CacheResult {
	return &CacheResult{
		succ: succ,
		data: data,
		err:  err,
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
		NewCacheErrorResult(err)
	}
	err = eng.SetString(key, val, values.NeverExpire)
	return NewCacheErrorResult(err)
}

func (eng *CacheEngine) ExecStrGet(args [][]byte) iface.Result {
	key := string(args[0])
	val, err := eng.Get(key)
	if err != nil {
		return NewCacheErrorResult(err)
	}
	return NewCacheResult(true, val.Bytes(), nil)
}

func (eng *CacheEngine) ExecDelKey(args [][]byte) iface.Result {
	key := string(args[0])
	err := eng.Del(key)
	return NewCacheErrorResult(err)
}

func (eng *CacheEngine) ExecExpire(args [][]byte) iface.Result {
	key := string(args[0])
	ttl, err := parseExpireKeyArgs(args)
	if err != nil {
		return NewCacheErrorResult(err)
	}
	err = eng.Expire(key, ttl)
	return NewCacheErrorResult(err)
}
