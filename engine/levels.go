package engine

import (
	"TikBase/engine/levels"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/errno"
	"errors"
)

type LevelEngine struct {
	*levels.Levels
}

func NewLevelEngine() (*LevelEngine, error) {
	ls, err := levels.New()
	if err != nil {
		return nil, err
	}

	return &LevelEngine{
		Levels: ls,
	}, nil
}

type LevelResult struct {
	succ bool
	data [][]byte
	err  error
}

func (r *LevelResult) Success() bool {
	return r.succ
}

func (r *LevelResult) Error() error {
	return r.err
}

func (r *LevelResult) Status() int {
	return 0
}

func (r *LevelResult) Data() [][]byte {
	return r.data
}

func NewSuccLevelResult() *LevelResult {
	return &LevelResult{
		succ: true,
	}
}

func NewUnknownLevelResult() *LevelResult {
	return &LevelResult{
		succ: false,
		err:  errors.New("unknown instruction type"),
	}
}

func (eng *LevelEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	switch ins {
	case iface.SET_STR:
		return eng.ExecSetString(args)
	case iface.GET_STR:
		return eng.ExecGetString(args)
	case iface.DEL:
		return eng.ExecDelKey(args)
	default:
		return NewUnknownLevelResult()
	}
}

func (eng *LevelEngine) ExecSetString(args [][]byte) *LevelResult {
	val := parseSetStringArgs(args)
	key := string(args[0])

	v := values.New([]byte(val), 0, iface.STRING)
	eng.Set(key, &v)
	return NewSuccLevelResult()
}

func (eng *LevelEngine) ExecGetString(args [][]byte) *LevelResult {
	key := string(args[0])
	val, ok := eng.Get(key)
	if !ok {
		return &LevelResult{
			succ: false,
			err:  errno.ErrKeyNotFound,
		}
	}
	return &LevelResult{
		succ: true,
		data: [][]byte{val.Bytes()},
	}
}

func (eng *LevelEngine) ExecDelKey(args [][]byte) *LevelResult {
	key := string(args[0])
	ok := eng.Del(key)
	if !ok {
		return &LevelResult{
			succ: false,
			err:  errno.ErrKeyNotFound,
		}
	}
	return &LevelResult{
		succ: true,
	}
}
