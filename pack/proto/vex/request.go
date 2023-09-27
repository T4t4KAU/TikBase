package vex

import (
	"encoding/binary"
	"io"
)

// 从reader中读取请求 解析出命令和参数
func readRequestFrom(reader io.Reader) (byte, [][]byte, error) {
	// 读取头部 指定具体大小
	header := make([]byte, headerLengthInProtocol)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return 0, nil, err
	}

	// 头部第一个字节为协议版本号
	version := header[0]
	if version != ProtocolVersion {
		return 0, nil, errProtocolVersionMismatch
	}

	// 头部第二字节是命令 后四字节是参数个数
	command := header[1]
	header = header[2:]

	// 所有的整数到字节数组的转换使用大段字节
	argsLength := binary.BigEndian.Uint32(header)
	args := make([][]byte, argsLength) // 读取参数
	if argsLength > 0 {
		// 读取参数长度 使用大端处理
		argLength := make([]byte, argsLengthInProtocol)
		for i := uint32(0); i < argsLength; i++ {
			// 读取数据
			_, err = io.ReadFull(reader, argLength)
			if err != nil {
				return 0, nil, err
			}
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
func writeRequestTo(writer io.Writer, command byte, args [][]byte) (int, error) {
	// 创建一个缓冲区 将协议版本号、命令和参数个数写入缓冲区
	req := make([]byte, headerLengthInProtocol)
	req[0] = ProtocolVersion
	req[1] = command
	binary.BigEndian.PutUint32(req[2:], uint32(len(args)))

	if len(args) > 0 {
		// 将参数添加到缓冲区
		argLength := make([]byte, argLengthInProtocol)
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
