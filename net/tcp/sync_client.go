package tcp

import (
	"TikCache/mode/caches"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
)

// Client TCP客户端
type Client struct {
	conn   net.Conn
	reader io.Reader
}

// NewClient 创建客户端
func NewClient(address string) (*Client, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

func (c *Client) exec(command byte, args [][]byte) ([]byte, error) {
	// 向连接写入命令
	_, err := writeRequestTo(c.conn, command, args)
	if err != nil {
		return nil, err
	}

	// 读取服务端返回的响应
	resp, body, err := readResponseFrom(c.reader)
	if resp == ErrorResp {
		// 返回错误
		return body, errors.New(string(body))
	}
	return body, nil
}

// Get 从缓存中获取指定键值对
func (c *Client) Get(key string) ([]byte, error) {
	return c.exec(getCommand, [][]byte{[]byte(key)})
}

// Set 添加键值对到缓存中
func (c *Client) Set(key string, value []byte, ttl int64) error {
	b := make([]byte, 8)
	// 使用大端形式储存数字
	binary.BigEndian.PutUint64(b, uint64(ttl))
	_, err := c.exec(setCommand, [][]byte{
		b, []byte(key), value,
	})
	return err
}

// Delete 删除指定键值对
func (c *Client) Delete(key string) error {
	_, err := c.exec(deleteCommand, [][]byte{[]byte(key)})
	return err
}

// Status 返回缓存状态
func (c *Client) Status() (*caches.Status, error) {
	body, err := c.exec(statusCommand, nil)
	if err != nil {
		return nil, err
	}
	status := caches.NewStatus()
	err = json.Unmarshal(body, status)
	return status, err
}

// Close 关闭客户端
func (c *Client) Close() error {
	return c.conn.Close()
}
