package iface

type Engine interface {
	Exec(command int, args [][]byte) Reply
	Close()
}

type KVStore interface {
	Get(key string) (Value, bool)
	Set(key string, value Value) bool
	Del(key string)
}

type DataEntity struct {
	Data any
}
