package engine

import (
	"TikBase/engine/bases"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/errorx"
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
		return eng.ExecSetString(args)
	case iface.GET_STR:
		return eng.ExecGetString(args)
	case iface.DEL:
		return eng.ExecDelKey(args)
	default:
		return NewUnknownBaseResult()
	}
}

func (eng *BaseEngine) ExecSetString(args [][]byte) iface.Result {
	val := values.New(args[1], 0, iface.STRING)
	ok := eng.SetBytes(args[0], &val)
	if !ok {
		return &BaseResult{
			succ: false,
			err:  errorx.ErrExceedCapacity,
		}
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecGetString(args [][]byte) iface.Result {
	val, ok := eng.Get(utils.BytesToString(args[0]))
	if !ok {
		return &BaseResult{
			succ: false,
			err:  errorx.ErrKeyNotFound,
		}
	}
	return &BaseResult{
		succ: true,
		data: [][]byte{val.Bytes()},
	}
}

func (eng *BaseEngine) ExecDelKey(args [][]byte) iface.Result {
	ok := eng.Del(utils.BytesToString(args[0]))
	if !ok {
		return &BaseResult{
			succ: false,
			err:  errorx.ErrKeyNotFound,
		}
	}

	return NewSuccBaseResult()
}
