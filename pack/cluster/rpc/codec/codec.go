package codec

import (
	"io"
)

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

type Type string

type NewCodeFunc func(closer io.ReadWriteCloser) Codec

// Header 消息头
type Header struct {
	ServiceMethod string // 方法名
	SeqNum        uint64 // 序列号
	Error         string // 错误信息
}

// Codec 消息编解码
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(any) error
	Write(*Header, any) error
}

var NewCodecFuncMap map[Type]NewCodeFunc

// 初始化
func init() {
	NewCodecFuncMap = make(map[Type]NewCodeFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
