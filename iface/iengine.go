package iface

import "github.com/T4t4KAU/TikBase/engine/data"

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

func (ins INS) String() string {
	if s, ok := insMap[ins]; ok {
		return s
	}
	return "UNKNOWN"
}

type Engine interface {
	Exec(ins INS, args [][]byte) Result
}

type KVStore interface {
	Get(key string) (Value, bool)
	Set(key string, value Value) bool
	Del(key string) bool
}

type Result interface {
	Success() bool
	Error() error
	Status() int
	Data() []byte
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
