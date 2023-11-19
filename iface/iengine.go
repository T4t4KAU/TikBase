package iface

import "TikBase/engine/data"

type INS int

const (
	ECHO INS = iota
	SET_STR
	SET_HASH
	ADD_SET
	ADD_ZSET
	PUSH_LIST

	GET_STR
	DEL
	EXPIRE
	KEYS
	NIL
)

func (ins INS) String() string {
	switch ins {
	case ECHO:
		return "ECHO"
	case SET_STR:
		return "SET"
	case GET_STR:
		return "GET"
	case DEL:
		return "DEL"
	case EXPIRE:
		return "EXPIRE"
	default:
		return "UNKNOWN"
	}
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
	Data() [][]byte
}

type Indexer interface {
	Get(key string) (Value, bool)
	Set(key string, value Value) bool
	Del(key string) bool
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
