package iface

type Type byte

const (
	NULL Type = iota
	PING
	STRING
	SET
	ZSET
	MAP
	LOG_POS
)

type Value interface {
	Score() float32 // 权值
	String() string
	Bytes() []byte
	Attr() Type
	Time() int64
	Alive() bool
}
