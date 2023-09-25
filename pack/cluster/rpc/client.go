package rpc

import (
	"TikCache/pack/cluster/rpc/codec"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Call 承载远程调用所需要的信息
type Call struct {
	SeqNum        uint64 // 序列号
	ServiceMethod string // 方法
	Args          any    // 参数
	Reply         any    // 回应
	Error         error
	Done          chan *Call
}

// 通知调用方
func (call *Call) done() {
	call.Done <- call
}

// Client 客户端
type Client struct {
	cc       codec.Codec
	opt      *Option    // 配置信息
	sm       sync.Mutex // send mutex
	header   codec.Header
	cm       sync.Mutex // common mutex
	seq      uint64     // 序列号
	closing  bool       // 标识连接是否关闭
	shutdown bool       // 标识服务是否停机

	pending map[uint64]*Call // 待执行调用
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shutdown")

// Close 关闭连接
func (cli *Client) Close() error {
	cli.cm.Lock()
	defer cli.cm.Unlock()

	if cli.closing {
		return ErrShutdown
	}
	cli.closing = true
	return cli.cc.Close()
}

// IsAvailable 检查客户端是否有效
func (cli *Client) IsAvailable() bool {
	cli.cm.Lock()
	defer cli.cm.Unlock()
	return !cli.shutdown && !cli.closing
}

// 注册调用
func (cli *Client) registerCall(call *Call) (uint64, error) {
	cli.cm.Lock()
	defer cli.cm.Unlock()

	// 检查是否关闭
	if cli.closing || cli.shutdown {
		return 0, rpc.ErrShutdown
	}

	call.SeqNum = cli.seq
	cli.pending[call.SeqNum] = call
	cli.seq++
	return call.SeqNum, nil
}

// 根据序列号 移除调用
func (cli *Client) removeCall(seq uint64) *Call {
	cli.cm.Lock()
	defer cli.cm.Unlock()

	call := cli.pending[seq]
	delete(cli.pending, seq)
	return call
}

// 服务端或客户端 发生错误调用
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

// 接收响应
func (cli *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		err = cli.cc.ReadHeader(&h)
		if err != nil {
			break
		}

		// 从pending中移除call
		call := cli.removeCall(h.SeqNum)
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

	cli.terminateCalls(err)
}

// NewClient 创建新的客户端
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("invalid codec type %s\n", opt.CodecType)
		return nil, fmt.Errorf("invalid codec type %s", opt.CodecType)
	}

	// 初始化编码器
	err := json.NewEncoder(conn).Encode(opt)
	if err != nil {
		log.Println("rpc client: options error:", err)
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// 初始化编解码器
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	cli := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}

	go cli.receive()
	return cli
}

// 解析配置选项
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than one")
	}
	opt := opts[0]
	opt.MagicNum = DefaultOption.MagicNum
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 连接RPC服务器
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// 发送请求
func (cli *Client) send(call *Call) {
	cli.sm.Lock()
	defer cli.sm.Unlock()

	// 注册调用方法
	seq, err := cli.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	cli.header.ServiceMethod = call.ServiceMethod
	cli.header.SeqNum = seq
	cli.header.Error = ""

	// 编码并发送请求
	err = cli.cc.Write(&cli.header, call.Args)
	if err != nil {
		call = cli.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// NewCall 创建远程调用
func (cli *Client) NewCall(method string, args, reply any, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: method,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}

	cli.send(call)
	return call
}

// Call 远程调用
func (cli *Client) Call(ctx context.Context, method string, args, reply any) error {
	call := cli.NewCall(method, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		cli.removeCall(call.SeqNum)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call = <-call.Done:
		return call.Error
	}
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (*Client, error)

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (*Client, error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	ch := make(chan clientResult)
	go func() {
		cli, e := f(conn, opt)
		ch <- clientResult{client: cli, err: e}
	}()

	if opt.ConnectTimeout == 0 {
		res := <-ch
		return res.client, res.err
	}

	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connection timeout: expect within %s", opt.ConnectTimeout)
	case res := <-ch:
		return res.client, res.err
	}
}
