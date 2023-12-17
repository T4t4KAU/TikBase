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

func buildBaseResp(succ bool, data [][]byte, err error) *BaseResult {
	return &BaseResult{
		succ: true,
		data: data,
		err:  err,
	}
}

func buildBaseErrResp(err error) *BaseResult {
	return &BaseResult{
		succ: false,
		err:  err,
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
		return &BaseResult{
			succ: false,
			err:  errno.ErrKeyIsEmpty,
		}
	}

	val := values.New(args[1], 0, iface.STRING)
	ok := eng.SetBytes(args[0], &val)
	if !ok {
		return &BaseResult{
			succ: false,
			err:  errno.ErrExceedCapacity,
		}
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecStrGet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return buildBaseErrResp(errno.ErrKeyIsEmpty)
	}

	val, ok := eng.Get(utils.B2S(args[0]))
	if !ok {
		return buildBaseErrResp(errno.ErrKeyNotFound)
	}
	return buildBaseResp(true, [][]byte{val.Bytes()}, nil)
}

func (eng *BaseEngine) ExecDelKey(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return buildBaseErrResp(errno.ErrKeyIsEmpty)
	}

	ok := eng.Del(utils.B2S(args[0]))
	if !ok {
		return &BaseResult{
			succ: false,
			err:  errno.ErrKeyNotFound,
		}
	}

	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecHashSet(args [][]byte) iface.Result {
	key, version, field, err := parseHashSetArgs(args)
	if err != nil {
		return buildBaseErrResp(err)
	}

	// 查找元数据
	meta, err := values.FindMeta(eng, key, iface.HASH)
	if err != nil {
		return buildBaseErrResp(err)
	}

	keyBytes := values.NewHashInternalKey(key, utils.B2I64(version), field).Encode()
	v, ok := eng.Get(utils.B2S(keyBytes))
	wb := eng.NewWriteBatch()

	// 不存在则创建更数据
	if !ok {
		meta.Size++
		_ = wb.Put(keyBytes, v.Bytes())
	}
	_ = wb.Put(keyBytes, v.Bytes())
	if err = wb.Commit(); err != nil {
		return buildBaseErrResp(err)
	}
	return buildBaseResp(true, [][]byte{v.Bytes()}, err)
}

func (eng *BaseEngine) ExecHashGet(args [][]byte) *BaseResult {
	key, field, err := parseHashGetArgs(args)
	if err != nil {
		return buildBaseErrResp(err)
	}
	meta, err := values.FindMeta(eng, key, iface.HASH)
	if err != nil {
		return buildBaseErrResp(err)
	}
	if meta.Size == 0 {
		return buildBaseErrResp(nil)
	}

	keyBytes := values.NewHashInternalKey(key, meta.Version, field).Encode()
	val, ok := eng.Get(utils.B2S(keyBytes))
	if !ok {
		return NewNotFoundBaseResult()
	}
	return buildBaseResp(true, [][]byte{val.Bytes()}, nil)
}

func (eng *BaseEngine) ExecHashDel(args [][]byte) *BaseResult {
	key, field, err := parseHashDelArgs(args)
	if err != nil {
		return buildBaseErrResp(err)
	}
	meta, err := values.FindMeta(eng, key, iface.HASH)
	if err != nil {
		return buildBaseErrResp(err)
	}
	if meta.Size == 0 {
		return buildBaseErrResp(nil)
	}

	// 检查数据是否存在
	keyBytes := values.NewHashInternalKey(key, meta.Version, field).Encode()
	_, ok := eng.Get(utils.B2S(keyBytes))
	if !ok {
		return NewNotFoundBaseResult()
	}

	wb := eng.NewWriteBatch()
	meta.Size--
	_ = wb.Put(key, meta.Encode())
	_ = wb.Delete(keyBytes)
	if err = wb.Commit(); err != nil {
		return buildBaseErrResp(err)
	}
	return NewSuccBaseResult()
}

func (eng *BaseEngine) ExecSetAdd(args [][]byte) *BaseResult {
	key, member, err := parseSetAddArgs(args)
	if err != nil {
		return buildBaseErrResp(err)
	}
	meta, err := values.FindMeta(eng, key, iface.SET)
	if err != nil {
		return buildBaseErrResp(err)
	}

	keyBytes := values.NewSetInternalKey(key, meta.Version, member).Encode()
	_, ok := eng.Get(utils.B2S(keyBytes))
	if !ok {
		wb := eng.NewWriteBatch()
		meta.Size++
		_ = wb.Put(keyBytes, meta.Encode())
		_ = wb.Put(keyBytes, nil)
		if err = wb.Commit(); err != nil {
			return buildBaseErrResp(err)
		}
	}

	return NewSuccBaseResult()
}
