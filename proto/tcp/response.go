package tcp

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// SuccessResp 响应码
	SuccessResp = 0
	ErrorResp   = 1
)

// 从reader中读取数据并解析出响应内容
func readResponseFrom(reader io.Reader) (reply byte, body []byte, err error) {
	// 读取指定字节数据
	header := make([]byte, headerLengthInProtocol)
	_, err = io.ReadFull(reader, header)
	if err != nil {
		return ErrorResp, nil, err
	}

	// 头部首字节：协议版本号
	version := header[0]
	if version != ProtocolVersion {
		return ErrorResp, nil, errors.New("response " + errProtocolVersionMismatch.Error())
	}

	// 从头部解析出响应码、响应体长度
	reply = header[1]
	header = header[2:]
	// 使用大端解析数字
	body = make([]byte, binary.BigEndian.Uint32(header))
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return ErrorResp, nil, err
	}
	return reply, body, nil
}

// 将响应写入到writer
func writeResponseTo(writer io.Writer, reply byte, body []byte) (int, error) {
	// 将响应体相关数据写入响应缓冲区并发送
	bodyLengthBytes := make([]byte, bodyLengthInProtocol)
	binary.BigEndian.PutUint32(bodyLengthBytes, uint32(len(body)))

	response := make([]byte, 2, headerLengthInProtocol+len(body))
	response[0] = ProtocolVersion
	response[1] = reply
	response = append(response, bodyLengthBytes...)
	response = append(response, body...)
	return writer.Write(response)
}

// 向writer写入错误信息
func writeErrorResponseTo(writer io.Writer, msg string) (int, error) {
	return writeResponseTo(writer, ErrorResp, []byte(msg))
}
