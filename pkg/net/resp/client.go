package resp

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/conc/wait"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

const (
	MaxRetryTimes = 3
)

const (
	created = iota
	running
	closed
)

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

type request struct {
	id        uint64
	args      []byte
	reply     iface.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

type Client struct {
	conn        net.Conn
	addr        string
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker

	status  int32
	working *sync.WaitGroup
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	cli := &Client{
		conn:        conn,
		addr:        addr,
		pendingReqs: make(chan *request),
		waitingReqs: make(chan *request),
		working:     &sync.WaitGroup{},
	}

	ch := make(chan struct{})
	go cli.Start(ch)
	<-ch

	return cli, nil
}

func (c *Client) Start(ch chan struct{}) {
	c.ticker = time.NewTicker(10 * time.Second)
	go c.handleWrite()
	go c.handleRead()
	go c.heartbeat()

	atomic.StoreInt32(&c.status, running)

	ch <- struct{}{}
}

func (c *Client) handleWrite() {
	for req := range c.pendingReqs {
		c.doRequest(req)
	}
}

func (c *Client) Get(key string) (string, error) {
	bytes := MakeGetRequest(key).ToBytes()
	reply := c.Send(bytes)
	if isErrReply(reply) {
		return "", errWithMsg(reply)
	}
	ss := strings.Split(utils.B2S(reply.ToBytes()), CRLF)
	if len(ss) < 1 {
		return "", nil
	}
	return ss[1], nil
}

func (c *Client) Set(key string, value string) error {
	bytes := MakeSetRequest(key, value).ToBytes()
	reply := c.Send(bytes)
	if isErrReply(reply) {
		return errWithMsg(reply)
	}
	return nil
}

func (c *Client) Del(key string) error {
	bytes := MakeDelRequest(key).ToBytes()
	reply := c.Send(bytes)
	if isErrReply(reply) {
		return errWithMsg(reply)
	}
	return nil
}

func (c *Client) Expire(key string, ttl int64) error {
	bytes := MakeExpireRequest(key, ttl).ToBytes()
	reply := c.Send(bytes)
	if isErrReply(reply) {
		return errWithMsg(reply)
	}
	return nil
}

func (c *Client) HGet(key, field string) (string, error) {
	bytes := MakeHGetRequest(key, field).ToBytes()
	reply := c.Send(bytes)

	if isErrReply(reply) {
		return "", errWithMsg(reply)
	}

	ss := strings.Split(utils.B2S(reply.ToBytes()), CRLF)
	if len(ss) < 1 {
		return "", nil
	}
	return ss[1], nil
}

func (c *Client) HSet(key, field, value string) error {
	bytes := MakeHSetRequest(key, field, value).ToBytes()
	reply := c.Send(bytes)
	if isErrReply(reply) {
		return errWithMsg(reply)
	}
	return nil
}

func (c *Client) Close() {
	atomic.StoreInt32(&c.status, closed)
	c.ticker.Stop()
	close(c.pendingReqs)

	c.working.Wait()

	_ = c.conn.Close()
	close(c.waitingReqs)
}

func (c *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}

	var err error
	for i := 0; i < MaxRetryTimes; i++ {
		_, err = c.conn.Write(req.args)
		if err == nil {
			break
		}
	}

	if err == nil {
		c.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (c *Client) finishRequest(reply iface.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()

	req := <-c.waitingReqs
	if req == nil {
		return
	}

	req.reply = reply
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (c *Client) reconnect() {
	_ = c.conn.Close()

	var conn net.Conn
	for i := 0; i < MaxRetryTimes; i++ {
		var err error
		conn, err = net.Dial("tcp", c.addr)
		if err != nil {
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}

	if conn == nil {
		c.Close()
		return
	}

	c.conn = conn
	for req := range c.waitingReqs {
		req.err = errno.ErrConnectionClosed
		req.waiting.Done()
	}

	c.waitingReqs = make(chan *request)
	go c.handleRead()
}

func (c *Client) handleRead() {
	ch := ParseStream(c.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&c.status)
			if status == closed {
				return
			}
			c.reconnect()
			return
		}
		c.finishRequest(payload.Data)
	}
}

func (c *Client) Send(args []byte) iface.Reply {
	if atomic.LoadInt32(&c.status) != running {
		return MakeErrReply("client closed")
	}

	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}

	req.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()
	c.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return MakeErrReply("server time out")
	}
	if req.err != nil {
		return MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (c *Client) heartbeat() {
	for range c.ticker.C {
		c.doHeartbeat()
	}
}

func (c *Client) doHeartbeat() {
	req := &request{
		args:      []byte("PING"),
		heartbeat: true,
		waiting:   &wait.Wait{},
	}

	req.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()
	c.pendingReqs <- req
	req.waiting.WaitWithTimeout(maxWait)
}

func errWithMsg(reply iface.Reply) error {
	return errors.New(strings.Trim(utils.B2S(reply.ToBytes()[1:]), CRLF))
}

func isErrReply(reply iface.Reply) bool {
	return string(reply.ToBytes()[0]) == "-"
}
