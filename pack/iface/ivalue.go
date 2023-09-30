package iface

type Type uint8

const (
	NULL Type = iota
	STRING
	SET
	ZSET
	MAP
)

type Value interface {
	Score() float32 // 权值
	String() string
	Bytes() []byte
	Attr() Type
	Time() int64
	Alive() bool
}
