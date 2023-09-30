package resp

import (
	"TikBase/pack/iface"
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

type Payload struct {
	Data iface.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (rs *readState) done() bool {
	return rs.expectedArgsCount > 0 && len(rs.args) >= rs.expectedArgsCount
}

func (rs *readState) clear() {
	*rs = readState{}
}

// ParseStream 解析数据流
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	var state readState
	var err error
	var msg []byte

	defer func() {
		if e := recover(); e != nil {

		}
	}()

	rd := bufio.NewReader(reader)
	for {
		var ioErr bool

		// 读取一行
		msg, ioErr, err = readLine(rd, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{
					Err: err,
				}

				// IO错误 关闭通道
				close(ch)
				return
			}
			ch <- &Payload{
				Err: err,
			}
			state.clear()
			continue
		}

		// 单行读取
		if !state.readingMultiLine {
			// 读取数组
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state.clear()
					continue
				}

				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &EmptyMultiBulkReply{},
					}
					state.clear()
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state.clear()
					continue
				}
				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &EmptyMultiBulkReply{},
					}
					state.clear()
					continue
				}
			} else {
				res, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: res,
					Err:  err,
				}
				state.clear()
				continue
			}
		} else {
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state.clear()
				continue
			}
			if state.done() {
				var result iface.Reply

				if state.msgType == '*' {
					result = MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = MakeBulkReply(state.args[0])
				}

				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state.clear()
			}
		}
	}
}

// 读取一行数据
func readLine(reader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 {
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error

	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}

	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// 解析多维数据
// example: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLen uint64

	// 获取预期数量
	expectedLen, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLen == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true // 读多行数据
		state.expectedArgsCount = int(expectedLen)
		state.args = make([][]byte, 0, expectedLen)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (iface.Reply, error) {
	var result iface.Reply

	s := strings.TrimSuffix(string(msg), CRLF)
	switch msg[0] {
	case '+':
		result = MakeStatusReply(s[1:])
	case '-':
		result = MakeErrReply(s[1:])
	case ':':
		val, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = MakeIntReply(val)
	}
	return result, nil
}

func readBody(msg []byte, state *readState) error {
	var err error

	line := msg[0 : len(msg)-2]
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
