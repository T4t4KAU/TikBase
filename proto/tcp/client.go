package tcp

import (
	"TikCache/engine/caches"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
)

type AsyncClient struct {
	*Client
	reqChan chan *request
}

// NewAsyncClient 创建异步客户端
func NewAsyncClient(address string) (*AsyncClient, error) {
	client, err := NewClient(address)
	if err != nil {
		return nil, err
	}
	c := &AsyncClient{
		Client:  client,
		reqChan: make(chan *request),
	}
	c.handleRequests()
	return c, nil
}

// 处理请求
func (c *AsyncClient) handleRequests() {
	go func() {
		for req := range c.reqChan {
			body, err := c.exec(req.command, req.args)
			req.resChan <- &Response{
				Body: body,
				Err:  err,
			}
		}
	}()
}

// 处理命令
func (c *AsyncClient) process(command byte, args [][]byte) <-chan *Response {
	resChan := make(chan *Response, 1)
	c.reqChan <- &request{
		command: command,
		args:    args,
		resChan: resChan,
	}
	return resChan
}

// Get 获取键值
func (c *AsyncClient) Get(key string) <-chan *Response {
	return c.process(getCommand, [][]byte{[]byte(key)})
}

// Set 设置键值
func (c *AsyncClient) Set(key string, value []byte, ttl int64) <-chan *Response {
	t := make([]byte, 8)
	binary.BigEndian.PutUint64(t, uint64(ttl))
	return c.process(setCommand, [][]byte{
		t, []byte(key), value,
	})
}

// Delete 删除键值
func (c *AsyncClient) Delete(key string) <-chan *Response {
	return c.process(deleteCommand, [][]byte{[]byte(key)})
}

func (c *AsyncClient) Status() <-chan *Response {
	return c.process(statusCommand, nil)
}

func (c *AsyncClient) Exit() error {
	close(c.reqChan)
	return c.Close()
}

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
