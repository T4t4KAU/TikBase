package resp

import (
	"bytes"
	"fmt"
	"github.com/T4t4KAU/TikBase/iface"
	"strconv"
)

/*

"+" 表示简单字符串
"-" 表示错误类型
":" 表示整数
"$" 表示 Bulk String
"*" 表述数组
*/

var CRLF = "\r\n"

// constant reply

// PongReply PONG响应
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

// OkReply OK响应
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

func MakeOkReply() *OkReply {
	return &OkReply{}
}

// NullBulkReply 空字符串响应
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// EmptyMultiBulkReply 空数组响应
type EmptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

func IsEmptyMultiBulkReply(reply iface.Reply) bool {
	return bytes.Equal(reply.ToBytes(), emptyMultiBulkBytes)
}

// NoReply 空响应
type NoReply struct{}

var noBytes = []byte("")

func (r *NoReply) ToBytes() []byte {
	return noBytes
}

func MakeNoReply() *NoReply {
	return &NoReply{}
}

// error reply

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (r *UnknownErrReply) Error() string {
	return "Err unknown"
}

func (r *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

type ArgNumErrReply struct {
	Command string
}

func (r *ArgNumErrReply) Error() string {
	return "ERR wrong number of arguments for '" + r.Command + "' command"
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Command + "' command\r\n")
}

func MakeArgNumErrReply(command string) *ArgNumErrReply {
	return &ArgNumErrReply{command}
}

type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func MakeSyntaxErrReply() *SyntaxErrReply {
	return &SyntaxErrReply{}
}

type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of values\r\n")

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of values"
}

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

type ProtocolErrReply struct {
	Message string
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Message + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error: '" + r.Message + "'\r\n"
}

func MakeProtocolErrReply(message string) *ProtocolErrReply {
	return &ProtocolErrReply{message}
}

type BulkReply struct {
	Arg []byte
}

func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

// MultiBulkReply 二维数组
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	var buff bytes.Buffer

	argLen := len(r.Args)
	buff.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buff.WriteString(string(nullBulkBytes) + CRLF)
		} else {
			buff.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buff.Bytes()
}

type IntReply struct {
	Code int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

type StatusReply struct {
	Status string
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

type StandardErrReply struct {
	Status string
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

type UnknownCommandErrReply struct {
	Command string
}

func (r *UnknownCommandErrReply) ToBytes() []byte {
	return []byte(fmt.Sprintf("-ERR unknown command '%s'", r.Command))
}

func MakeUnknownCommandErrReply(command []byte) *UnknownCommandErrReply {
	return &UnknownCommandErrReply{
		Command: string(command),
	}
}

func IsErrReply(reply iface.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
