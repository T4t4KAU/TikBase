package engine

import (
	"errors"
	"github.com/T4t4KAU/TikBase/engine/bases"
	"github.com/T4t4KAU/TikBase/engine/values"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/config"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

type BaseEngine struct {
	*bases.Base
	execFunc map[iface.INS]ExecFunc
}

type BaseResult struct {
	succ bool
	data []byte
	err  error
}

func (r *BaseResult) Data() []byte {
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

func (r *BaseResult) String() string {
	return utils.B2S(r.data)
}

func NewBaseResult(succ bool, data []byte, err error) *BaseResult {
	return &BaseResult{
		succ: true,
		data: data,
		err:  err,
	}
}

func NewBaseErrResult(err error) *BaseResult {
	if err == nil {
		return NewSuccBaseResult()
	}
	return &BaseResult{
		succ: false,
		err:  err,
	}
}

func NewBaseResultFromValue(value iface.Value) *BaseResult {
	return &BaseResult{
		succ: true,
		err:  nil,
		data: value.Bytes(),
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

func NewBaseEngineWith(config config.BaseStoreConfig) (*BaseEngine, error) {
	option := bases.Options{
		DirPath:            config.Directory,
		DataFileSize:       int64(config.DatafileSize),
		SyncWrites:         config.SyncWrites,
		IndexType:          bases.NewIndexerType(config.Indexer),
		BytesPerSync:       uint(config.BytesPerSync),
		MMapAtStartup:      config.MmapAtStartup,
		DataFileMergeRatio: float32(config.DatafileMergeRatio),
	}

	base, err := bases.NewBaseWith(option)
	if err != nil {
		panic(err)
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
	eng.registerExecFunc(iface.DEL_HASH, eng.ExecHashDel)
	eng.registerExecFunc(iface.LEFT_PUSH_LIST, eng.ExecListLeftPush)
	eng.registerExecFunc(iface.LEFT_POP_LIST, eng.ExecListLeftPop)
	eng.registerExecFunc(iface.ADD_SET, eng.ExecSetAdd)
	eng.registerExecFunc(iface.REM_SET, eng.ExecSetRem)
	eng.registerExecFunc(iface.IS_MEMBER_SET, eng.ExecSetIsMember)
}

func (eng *BaseEngine) ExecStrSet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return NewBaseErrResult(errno.ErrKeyIsEmpty)
	}

	val := values.New(args[1], 0, iface.STRING)
	err := eng.Set(utils.B2S(args[0]), &val)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecStrGet(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return NewBaseErrResult(errno.ErrKeyIsEmpty)
	}

	val, err := eng.Get(utils.B2S(args[0]))
	if err != nil {
		return NewNotFoundBaseResult()
	}
	return NewBaseResult(true, val.Bytes(), nil)
}

func (eng *BaseEngine) ExecDelKey(args [][]byte) iface.Result {
	if len(args[0]) <= 0 {
		return NewBaseErrResult(errno.ErrKeyIsEmpty)
	}

	err := eng.Del(utils.B2S(args[0]))
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecHashSet(args [][]byte) iface.Result {
	key, field, value, err := ParseHashSetArgs(args)
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
	key, field, err := ParseHashGetArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	v, err := eng.HGet(key, field)
	if err != nil {
		return NewBaseErrResult(err)
	}
	return NewBaseResultFromValue(v)
}

func (eng *BaseEngine) ExecHashDel(args [][]byte) iface.Result {
	key, field, err := ParseHashDelArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.HDel(key, field)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecListLeftPush(args [][]byte) iface.Result {
	key, element, err := ParseListPushArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.LPush(key, element)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecListRightPush(args [][]byte) iface.Result {
	key, element, err := ParseListPushArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.RPush(key, element)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecListLeftPop(args [][]byte) iface.Result {
	key, err := ParseListPopArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	v, err := eng.LPop(key)
	if err != nil {
		return NewBaseErrResult(err)
	}
	return NewBaseResultFromValue(v)
}

func (eng *BaseEngine) ExecListRightPop(args [][]byte) iface.Result {
	key, err := ParseListPopArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	v, err := eng.RPop(key)
	if err != nil {
		return NewBaseErrResult(err)
	}
	return NewBaseResultFromValue(v)
}

func (eng *BaseEngine) ExecSetAdd(args [][]byte) iface.Result {
	key, value, err := ParseSetAddArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.SAdd(key, value)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecSetRem(args [][]byte) iface.Result {
	key, member, err := ParseSetRemArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.SRem(key, member)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecSetIsMember(args [][]byte) iface.Result {
	key, member, err := ParseSetIsMemberArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.SIsMember(key, member)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) ExecZSetAdd(args [][]byte) iface.Result {
	key, score, member, err := ParseZSetAddArgs(args)
	if err != nil {
		return NewBaseErrResult(err)
	}
	_, err = eng.ZAdd(key, score, member)
	return NewBaseErrResult(err)
}

func (eng *BaseEngine) RecoverFromBytes(data []byte) error {
	return eng.Base.RecoverFromBytes(data)
}
