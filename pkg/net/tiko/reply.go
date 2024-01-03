package tiko

import (
	"encoding/binary"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"io"
)

// 从reader中读取数据并解析出回复内容
func parseReply(reader io.Reader) (byte, []byte, error) {
	// 读取指定字节数据
	header := make([]byte, HeaderLength)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return Error, nil, err
	}

	// 头部首字节：协议版本号
	version := header[0]
	if version != Version {
		return Error, nil, protocolError()
	}

	// 从头部解析出响应码、响应体长度
	code := header[1]
	header = header[2:]
	// 使用大端解析数字
	body := make([]byte, binary.BigEndian.Uint32(header))
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return Error, nil, err
	}
	return code, body, nil
}

// 将响应写入到writer
func writeReply(writer io.Writer, code byte, body []byte) (int, error) {
	// 将响应体相关数据写入响应缓冲区并发送
	bodyLengthBytes := make([]byte, BodyLength)
	binary.BigEndian.PutUint32(bodyLengthBytes, uint32(len(body)))

	data := make([]byte, 2, HeaderLength+len(body))
	data[0] = Version
	data[1] = code
	data = append(data, bodyLengthBytes...)
	data = append(data, body...)
	return writer.Write(data)
}

// 向writer写入错误信息
func writeErrorReply(writer io.Writer, msg string) (int, error) {
	return writeReply(writer, Error, utils.S2B(msg))
}
