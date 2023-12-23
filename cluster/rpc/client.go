package rpc

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/T4t4KAU/TikBase/cluster/rpc/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Call represents an active RPC.
type Call struct {
	Seq           uint64
	ServiceMethod string      // 调用格式为"服务名.方法名"
	Args          interface{} // 函数参数
	Reply         interface{} // 回复
	Error         error       // 错误
	Done          chan *Call  // 调用
}

// 通知调用完成
func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec // 编解码器
	opt      *Option
	sm       sync.Mutex // sending mutex
	header   codec.Header
	cm       sync.Mutex // common mutex
	seq      uint64
	pending  map[uint64]*Call // 就绪	closing  bool
	shutdown bool
	closing  bool
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close the connection
func (cli *Client) Close() error {
	cli.cm.Lock()
	defer cli.cm.Unlock()
	if cli.closing {
		return ErrShutdown
	}
	cli.closing = true
	return cli.cc.Close()
}

// IsAvailable return true if the client does work
func (cli *Client) IsAvailable() bool {
	cli.cm.Lock()
	defer cli.cm.Unlock()
	return !cli.shutdown && !cli.closing
}

// register a call
func (cli *Client) registerCall(call *Call) (uint64, error) {
	cli.cm.Lock()
	defer cli.cm.Unlock()
	if cli.closing || cli.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = cli.seq
	// 添加调用
	cli.pending[call.Seq] = call
	cli.seq++
	return call.Seq, nil
}

func (cli *Client) removeCall(seq uint64) *Call {
	cli.cm.Lock()
	defer cli.cm.Unlock()
	call := cli.pending[seq]
	delete(cli.pending, seq)
	return call
}

// 中断调用
func (cli *Client) terminateCalls(err error) {
	cli.sm.Lock()
	defer cli.sm.Unlock()
	cli.cm.Lock()
	defer cli.cm.Unlock()
	cli.shutdown = true
	for _, call := range cli.pending {
		call.Error = err
		call.done()
	}
}

// 发送调用
func (cli *Client) send(call *Call) {
	// make sure that the client will send a complete request
	cli.sm.Lock()
	defer cli.sm.Unlock()

	// register this call.
	seq, err := cli.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// prepare request header
	cli.header.ServiceMethod = call.ServiceMethod
	cli.header.Seq = seq
	cli.header.Error = ""

	// encode and send the request
	if err = cli.cc.Write(&cli.header, call.Args); err != nil {
		c := cli.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if c != nil {
			c.Error = err
			c.done()
		}
	}
}

func (cli *Client) recv() {
	var err error
	for err == nil {
		var h codec.Header
		if err = cli.cc.ReadHeader(&h); err != nil {
			break
		}
		call := cli.removeCall(h.Seq)
		switch {
		case call == nil:
			err = cli.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = cli.cc.ReadBody(nil)
			call.done()
		default:
			err = cli.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// 发生错误 中断调用
	cli.terminateCalls(err)
}

// Invoke invokes the function asynchronously.
// It returns the Call structure representing the invocation.
func (cli *Client) Invoke(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	cli.send(call)
	return call
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
func (cli *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := cli.Invoke(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		cli.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call = <-call.Done:
		return call.Error
	}
}

func parseOptions(opts ...*Option) (*Option, error) {
	// if opts is nil or pass nil as parameter
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

func NewClient(conn net.Conn, opt *Option) (client *Client, err error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err = fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return
	}
	// send options with server
	if err = gob.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		return
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}

	// 异步接收消息
	go client.recv()
	return client
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

func dialTimeout(fn newClientFunc, network, address string, opts ...*Option) (*Client, error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connection if client is nil
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		cli, e := fn(conn, opt)
		ch <- clientResult{client: cli, err: e}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial connects to an RPC server at the specified network address
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}
