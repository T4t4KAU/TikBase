package iface

type Type uint8

const (
	INT Type = iota
	FLOAT
	STRING
	SET
	ZSET
	MAP
	NULL
)

type Value interface {
	Score() float32 // 权值
	String() string
	Bytes() []byte
}
