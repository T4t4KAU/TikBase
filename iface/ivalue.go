package iface

type Type byte

const (
	NULL Type = iota
	PING
	STRING
	SET
	ZSET
	HASH
	LIST
	LOG_POS
	META_DATA
)

type Value interface {
	Score() float32 // 权值
	String() string
	Bytes() []byte
	Attr() Type
	Time() int64
	Alive() bool
}
