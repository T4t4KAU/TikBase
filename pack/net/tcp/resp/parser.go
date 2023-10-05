package resp

import (
	"TikBase/iface"
	"TikBase/pack/tlog"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
)

type Payload struct {
	Data iface.Reply
	Err  error
}

// ParseStream 解析数据流
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		close(ch)
		if err := recover(); err != nil {
			tlog.Error(err, string(debug.Stack()))
		}
	}()
	reader := bufio.NewReader(rawReader)
	for {
		// 获取一行数据
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}

			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			protocolError(ch, line)
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			ch <- &Payload{
				Data: MakeStatusReply(string(line[1:])),
			}
		case '-':
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, line)
				continue
			}
			ch <- &Payload{
				Data: MakeIntReply(value),
			}
		case '$':
			err = parseBulk(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				return
			}
		case '*':
			err = parseMultiBulk(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: MakeMultiBulkReply(args),
			}
		}
	}
}

func parseBulk(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 获取字符串长度
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, header)
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: MakeNullBulkReply(),
		}
		return nil
	}

	// 读取剩下数据
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

// example: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func parseMultiBulk(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 获取数组元素个数
	n, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || n < 0 {
		protocolError(ch, header)
		return nil
	} else if n == 0 {
		ch <- &Payload{
			Data: MakeEmptyMultiBulkReply(),
		}
		return nil
	}

	lines := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, line)
			break
		}

		// 读取单个字符串长度
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, header)
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err = io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	ch <- &Payload{
		Data: MakeMultiBulkReply(lines),
	}
	return nil
}

func protocolError(ch chan<- *Payload, msg []byte) {
	err := errors.New(fmt.Sprintf("Protocol error: %s", string(msg)))
	ch <- &Payload{Err: err}
}
