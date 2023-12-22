package resp

import (
	"TikBase/iface"
	"TikBase/pack/tlog"
	"TikBase/pack/utils"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

var (
	errProtocolError = fmt.Errorf("protocol error")
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
			tlog.Error(err, utils.B2S(debug.Stack()))
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
				Data: MakeStatusReply(utils.B2S(line[1:])),
			}
		case '-':
			ch <- &Payload{
				Err: MakeErrReply(utils.B2S(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(utils.B2S(line[1:]), 10, 64)
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

func readLine(rawReader io.Reader) (payloads []*Payload) {
	payloads = make([]*Payload, 0, 1)
	reader := bufio.NewReader(rawReader)
	for {
		payload := &Payload{}
		// 获取一行数据
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				return
			}
			payload.Err = err
			payloads = append(payloads, payload)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			payload.Err = errProtocolError
			goto END
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			payload.Data = MakeStatusReply(utils.B2S(line[1:]))
			goto END
		case '-':
			payload.Err = MakeErrReply(utils.B2S(line[1:]))
			goto END
		case ':':
			value, err := strconv.ParseInt(utils.B2S(line[1:]), 10, 64)
			if err != nil {
				payload.Err = err
				goto END
			}
			payload.Data = MakeIntReply(value)
		case '$':
			err = readBulk(line, reader, payloads)
			if err != nil {
				payload.Err = err
				payloads = append(payloads, payload)
				return
			}
		case '*':
			err = readMultiBulk(line, reader, &payloads)
			if err != nil {
				payload.Err = err
				payloads = append(payloads, payload)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			payload.Data = MakeMultiBulkReply(args)
		}
	END:
		if payload.Err != nil || payload.Data != nil {
			payloads = append(payloads, payload)
			return
		}
	}
}

func parseBulk(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 获取字符串长度
	strLen, err := strconv.ParseInt(utils.B2S(header[1:]), 10, 64)
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

func readBulk(header []byte, reader *bufio.Reader, payloads []*Payload) error {
	payload := &Payload{}

	defer func() {
		payloads = append(payloads, payload)
	}()

	// 获取字符串长度
	strLen, err := strconv.ParseInt(utils.B2S(header[1:]), 10, 64)
	if err != nil {
		payload.Err = err
		return nil
	} else if strLen == -1 {
		payload.Data = MakeNullBulkReply()
		return nil
	}
	// 读取剩下数据
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		payload.Err = err
		return err
	}
	payload.Data = MakeBulkReply(body[:len(body)-2])
	return nil
}

// example: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func parseMultiBulk(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 获取数组元素个数
	n, err := strconv.ParseInt(utils.B2S(header[1:]), 10, 64)
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
		strLen, err := strconv.ParseInt(utils.B2S(line[1:length-2]), 10, 64)
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

func readMultiBulk(header []byte, reader *bufio.Reader, payloads *[]*Payload) error {
	payload := &Payload{}

	// 获取数组元素个数
	n, err := strconv.ParseInt(utils.B2S(header[1:]), 10, 64)
	if err != nil || n < 0 {
		payload.Err = err
		*payloads = append(*payloads, payload)
		return nil
	} else if n == 0 {
		payload.Data = MakeNullBulkReply()
		*payloads = append(*payloads, payload)
		return nil
	}
	lines := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return nil
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			payload.Err = errProtocolError
			break
		}

		// 读取单个字符串长度
		strLen, err := strconv.ParseInt(utils.B2S(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			payload.Err = err
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
	payload.Data = MakeMultiBulkReply(lines)
	*payloads = append(*payloads, payload)
	return nil
}

func protocolError(ch chan<- *Payload, msg []byte) {
	err := fmt.Errorf("protocol error: %s", string(msg))
	ch <- &Payload{Err: err}
}