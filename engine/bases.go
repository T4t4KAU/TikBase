package engine

import (
	"TikBase/engine/bases"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/errno"
	"TikBase/pack/utils"
	"errors"
)

type BaseEngine struct {
	*bases.Base
	execFunc map[iface.INS]ExecFunc
}

type BaseResult struct {
	succ bool
	data [][]byte
	err  error
}

func (r *BaseResult) Data() [][]byte {
	return r.data
}

func (r *BaseResult) Success() bool {
	return r.succ
}

func (r *BaseResult) Error() error {
	return r.err
}

func (r *BaseResult) Status() int {
	return 0
}

func buildBaseResult(succ bool, data [][]byte, err error) *BaseResult {
	return &BaseResult{
		succ: true,
		data: data,
		err:  err,
	}
}

func NewBaseErrResult(err error) *BaseResult {
	return &BaseResult{
		succ: false,
		err:  err,
	}
}

func NewBaseResultFromValue(value iface.Value) *BaseResult {
	return &BaseResult{
		succ: true,
		err:  nil,
		data: [][]byte{value.Bytes()},
	}
}

func NewSuccBaseResult() *BaseResult {
	return &BaseResult{
		succ: true,
	}
}

func NewUnknownBaseResult() *BaseResult {
	return &BaseResult{
		succ: false,
		err:  errors.New("unknown instruction type"),
	}
}

func NewNotFoundBaseResult() *BaseResult {
	return &BaseResult{
		succ: false,
		err:  errno.ErrKeyNotFound,
	}
}

func NewBaseEngine() (*BaseEngine, error) {
	base, err := bases.New()
	if err != nil {
		return nil, err
	}

	eng := &BaseEngine{
		Base:     base,
		execFunc: make(map[iface.INS]ExecFunc),
	}
	eng.initExecFunc()

	return eng, nil
}

func (eng *BaseEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	if fn, ok := eng.execFunc[ins]; ok {
		return fn(args)
	}
	return NewUnknownBaseResult()
}

func (eng *BaseEngine) registerExecFunc(ins iface.INS, fn ExecFunc) {
	eng.execFunc[ins] = fn
}

func (eng *BaseEngine) initExecFunc() {
	eng.registerExecFunc(iface.GET_STR, eng.ExecStrGet)
	eng.registerExecFunc(iface.SET_STR, eng.ExecStrSet)
	eng.registerExecFunc(iface.DEL, eng.ExecDelKey)
	eng.registerExecFunc(iface.SET_HASH, eng.ExecHashSet)
	eng.registerExecFunc(iface.GET_HASH, eng.ExecHashGet)
	eng.registerExecFunc(iface.PUSH_LIST, eng.ExecListPush)
	eng.registerExecFunc(iface.POP_LIST, eng.ExecListPop)
}

func (eng *BaseEngine) ExecStrSet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return NewBaseErrResult(errno.ErrKeyIsEmpty)
	}

	val := values.New(args[1], 0, iface.STRING)
	ok := eng.SetBytes(args[0], &val)
	if !ok {
		return NewBaseErrResult(errno.ErrExceedCapacity)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecStrGet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return NewBaseErrResult(errno.ErrKeyIsEmpty)
	}

	val, ok := eng.Get(utils.B2S(args[0]))
	if !ok {
		return NewBaseErrResult(errno.ErrKeyNotFound)
	}
	return buildBaseResult(true, [][]byte{val.Bytes()}, nil)
}

func (eng *BaseEngine) ExecDelKey(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return NewBaseErrResult(errno.ErrKeyIsEmpty)
	}

	ok := eng.Del(utils.B2S(args[0]))
	if !ok {
		return NewNotFoundBaseResult()
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecHashSet(args [][]byte) iface.Result {
	key, field, value, err := parseHashSetArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.HSet(key, field, value)
	if err != nil {
		return NewBaseErrResult(err)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecHashGet(args [][]byte) iface.Result {
	key, field, err := parseHashGetArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	v, ok := eng.HGet(key, field)
	if !ok {
		return NewNotFoundBaseResult()
	}
	return NewBaseResultFromValue(v)
}

func (eng *BaseEngine) ExecListPush(args [][]byte) iface.Result {
	key, element, err := parseListPushArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.LPush(key, element)
	if err != nil {
		return NewBaseErrResult(err)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecListPop(args [][]byte) iface.Result {
	key, err := parseListPopArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	v, err := eng.LPop(key)
	if err != nil {
		return NewBaseErrResult(err)
	}
	return NewBaseResultFromValue(v)
}
