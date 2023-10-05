package tiko

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

const (
	getCommand    = byte(1)
	setCommand    = byte(2)
	deleteCommand = byte(3)
	statusCommand = byte(4)
)

var (
	errCommandNeedsMoreArguments = errors.New("command needs more arguments")
	errNotFound                  = errors.New("not found")
	errProtocolVersionMismatch   = errors.New("protocol version between client and proto doesn't match")
	errCommandNotFound           = errors.New("failed to find a handler of command")
)

const (
	Version      = byte(1)
	HeaderLength = 6
	ArgsLength   = 4
	ArgLength    = 4
	BodyLength   = 4
)

func isClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}

const (
	Success = 0
	Error   = 1
)

func parseRequest(reader io.Reader) (byte, [][]byte, error) {
	// 读取头部 指定具体大小
	header := make([]byte, HeaderLength)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return Error, nil, err
	}

	// 头部第一个字节为协议版本号
	version := header[0]
	if version != Version {
		return Error, nil, errProtocolVersionMismatch
	}

	// 头部第二字节是命令 后四字节是参数个数
	command := header[1]
	header = header[2:]

	// 所有的整数到字节数组的转换使用大段字节
	argsLength := binary.BigEndian.Uint32(header)
	args := make([][]byte, argsLength) // 读取参数
	if argsLength > 0 {
		// 读取参数长度 使用大端处理
		argLength := make([]byte, ArgLength)
		for i := uint32(0); i < argsLength; i++ {
			// 读取参数长度
			_, err = io.ReadFull(reader, argLength)
			if err != nil {
				return 0, nil, err
			}

			// 接收参数
			arg := make([]byte, binary.BigEndian.Uint32(argLength))
			_, err = io.ReadFull(reader, arg)
			if err != nil {
				return 0, nil, err
			}
			args[i] = arg
		}
	}
	return command, args, nil
}

// 将请求写入到writer中
func writeRequest(writer io.Writer, command byte, args [][]byte) (int, error) {
	// 创建一个缓冲区 将协议版本号、命令和参数个数写入缓冲区
	req := make([]byte, HeaderLength)
	req[0] = Version
	req[1] = command
	binary.BigEndian.PutUint32(req[2:], uint32(len(args)))

	if len(args) > 0 {
		// 将参数添加到缓冲区
		argLength := make([]byte, ArgLength)
		for _, arg := range args {
			// 大端存储
			binary.BigEndian.PutUint32(argLength, uint32(len(arg)))
			req = append(req, argLength...)
			req = append(req, arg...)
		}
	}

	// 向请求写入数据
	return writer.Write(req)
}

type GetRequest struct {
	Key string
}

func MakeGetRequest(key string) *GetRequest {
	return &GetRequest{
		Key: key,
	}
}

func (req *GetRequest) Bytes() []byte {
	data := make([]byte, HeaderLength)
	data[0] = Version
	data[1] = getCommand
	binary.BigEndian.PutUint32(data[2:], 1)

	// 将参数添加到缓冲区
	argLength := make([]byte, ArgLength)
	binary.BigEndian.PutUint32(argLength, uint32(len([]byte(req.Key))))
	data = append(data, argLength...)
	data = append(data, req.Key...)

	return data
}

func WriteGetRequest(writer io.Writer, key []byte) (int, error) {
	return writeRequest(writer, getCommand, [][]byte{key})
}

type SetRequest struct {
	Key   string
	Value string
}

func MakeSetRequest(key, value string) *SetRequest {
	return &SetRequest{
		Key:   key,
		Value: value,
	}
}

func (req *SetRequest) Bytes() []byte {
	data := make([]byte, HeaderLength)
	data[0] = Version
	data[1] = setCommand
	binary.BigEndian.PutUint32(data[2:], 2)

	// 将参数添加到缓冲区
	argLength := make([]byte, ArgLength)
	binary.BigEndian.PutUint32(argLength, uint32(len([]byte(req.Key))))
	data = append(data, argLength...)
	data = append(data, req.Key...)
	binary.BigEndian.PutUint32(argLength, uint32(len([]byte(req.Value))))
	data = append(data, argLength...)
	data = append(data, req.Value...)

	return data
}

func WriteSetRequest(writer io.Writer, key []byte, value []byte) (int, error) {
	return writeRequest(writer, setCommand, [][]byte{key, value})
}

func protocolError() error {
	return errors.New("response " + errProtocolVersionMismatch.Error())
}
