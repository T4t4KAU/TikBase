package iface

type INS int

const (
	ECHO INS = iota
	SET_STR
	SET_HASH
	ADD_SET
	ADD_ZSET
	PUSH_LIST

	GET_STR
	DEL_STR
)

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
