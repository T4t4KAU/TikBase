package iface

import (
	"encoding/json"
	"github.com/T4t4KAU/TikBase/engine/data"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

type INS int

const (
	ECHO INS = iota
	SET_STR
	SET_HASH
	ADD_SET
	ADD_ZSET
	LEFT_PUSH_LIST
	RIGHT_PUSH_LIST
	LEFT_POP_LIST
	RIGHT_POP_LIST

	GET_STR
	GET_HASH
	DEL
	DEL_HASH
	REM_SET
	IS_MEMBER_SET

	EXPIRE
	KEYS
	NIL
)

var ternaryIns = map[INS]struct{}{
	GET_HASH: {},
	SET_HASH: {},
	DEL_HASH: {},
}

var binaryIns = map[INS]struct{}{
	SET_STR: {},
	GET_STR: {},
	DEL:     {},
}

func (ins INS) BIN() bool {
	_, ok := binaryIns[ins]
	return ok
}

func (ins INS) TER() bool {
	_, ok := ternaryIns[ins]
	return ok
}

func (ins INS) String() string {
	if s, ok := insMap[ins]; ok {
		return s
	}
	return "UNKNOWN"
}

type Engine interface {
	Exec(ins INS, args [][]byte) Result // 执行指令
	Snapshot() ([]byte, error)          // 生成快照
	RecoverFromBytes(data []byte) error // 恢复数据
}

type KVStore interface {
	Get(key string) (Value, error)
	Set(key string, value Value) error
	Del(key string) error
}

type Result interface {
	Success() bool
	Error() error
	Status() int
	Data() []byte
	String() string
}

// Indexer 抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
}

type Iterator interface {
	// 回到起点
	Rewind()

	// 根据传入的key查找到第一个大于或小于等于的目标key
	Seek(key []byte)

	// 跳转到下一个key
	Next()

	// Valid 是否有效 即是否已经遍历完了所有key
	Valid() bool

	Key() []byte

	Value() *data.LogRecordPos

	Close()
}

var insMap = map[INS]string{
	ECHO:    "ECHO",
	SET_STR: "SET",
	GET_STR: "GET",
	DEL:     "DEL",
	EXPIRE:  "EXPIRE",
}

type IWriteBatch interface {
	Commit() error
}

type IService interface {
	Start() error // 启动服务
	Name() string // 服务名称
}

type Filter interface {
	Add(key []byte)
	Exist(bitmap, key []byte) bool
	Hash() []byte
	Reset()
	KeyLen() int
}

// Command 复制状态机指令
type Command struct {
	Ins   INS    `json:"op,omitempty"`    // 指令
	Key   string `json:"key,omitempty"`   // 键
	Field string `json:"field,omitempty"` // 字段
	Value []byte `json:"value,omitempty"` // 值
}

// Encode 将指令编码
func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c Command) Args() [][]byte {
	return utils.KeyValueBytes(c.Key, c.Value)
}
