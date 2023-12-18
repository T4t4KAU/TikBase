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

func buildBaseErrResult(err error) *BaseResult {
	return &BaseResult{
		succ: false,
		err:  err,
	}
}

func buildBaseResultFromValue(value iface.Value) *BaseResult {
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
	return &BaseEngine{
		Base: base,
	}, nil
}

func (eng *BaseEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	switch ins {
	case iface.SET_STR:
		return eng.ExecStrSet(args)
	case iface.GET_STR:
		return eng.ExecStrGet(args)
	case iface.DEL:
		return eng.ExecDelKey(args)
	default:
		return NewUnknownBaseResult()
	}
}

func (eng *BaseEngine) ExecStrSet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return buildBaseErrResult(errno.ErrKeyIsEmpty)
	}

	val := values.New(args[1], 0, iface.STRING)
	ok := eng.SetBytes(args[0], &val)
	if !ok {
		return buildBaseErrResult(errno.ErrExceedCapacity)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecStrGet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return buildBaseErrResult(errno.ErrKeyIsEmpty)
	}

	val, ok := eng.Get(utils.B2S(args[0]))
	if !ok {
		return buildBaseErrResult(errno.ErrKeyNotFound)
	}
	return buildBaseResult(true, [][]byte{val.Bytes()}, nil)
}

func (eng *BaseEngine) ExecDelKey(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return buildBaseErrResult(errno.ErrKeyIsEmpty)
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
		return buildBaseErrResult(err)
	}
	_, err = eng.HSet(key, field, value)
	if err != nil {
		return buildBaseErrResult(err)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecHashGet(args [][]byte) iface.Result {
	key, field, err := parseHashGetArgs(args)
	if err != nil {
		return buildBaseErrResult(err)
	}
	v, ok := eng.HGet(key, field)
	if !ok {
		return NewNotFoundBaseResult()
	}
	return buildBaseResultFromValue(v)
}

func (eng *BaseEngine) ExecListPush(args [][]byte) iface.Result {
	key, element, err := parseListPushArgs(args)
	if err != nil {
		return buildBaseErrResult(err)
	}
	_, err = eng.LPush(key, element)
	if err != nil {
		return buildBaseErrResult(err)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecListPop(args [][]byte) iface.Result {
	key, err := parseListPopArgs(args)
	if err != nil {
		return buildBaseErrResult(err)
	}
	v, err := eng.LPop(key)
	if err != nil {
		return buildBaseErrResult(err)
	}
	return buildBaseResultFromValue(v)
}
