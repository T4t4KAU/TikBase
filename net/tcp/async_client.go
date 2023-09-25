package tcp

import "encoding/binary"

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
