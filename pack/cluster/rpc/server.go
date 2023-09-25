package rpc

import (
	"TikCache/pack/cluster/rpc/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNum = 0x3bef5c

// Option 设置消息编解码方式
type Option struct {
	MagicNum       int           // 标识协议类型
	CodecType      codec.Type    // 标识编码类型
	ConnectTimeout time.Duration // 连接超时时间
	HandleTimeout  time.Duration // 处理超时时间
}

// DefaultOption 默认配置
var DefaultOption = &Option{
	MagicNum:       MagicNum,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

type Server struct {
	serviceMap sync.Map
}

// NewServer 创建服务器
func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// ServeConn 处理连接
func (srv *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		conn.Close()
	}()

	var opt Option
	err := json.NewDecoder(conn).Decode(&opt)
	if err != nil {
		return
	}
	if opt.MagicNum != MagicNum {
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	srv.serveCodec(f(conn), &opt)
}

// 处理编码
func (srv *Server) serveCodec(cc codec.Codec, opt *Option) {
	mutex := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		// 读取请求
		req, err := srv.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.header.Error = err.Error()
			// 回复请求
			srv.sendResponse(cc, req.header, invalidRequest, mutex)
			continue
		}
		wg.Add(1)
		// 处理请求
		go srv.handleRequest(cc, req, mutex, wg, opt.HandleTimeout)
	}
	wg.Wait()
	cc.Close()
}

// Accept 接收请求
func (srv *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		go srv.ServeConn(conn)
	}
}

func (srv *Server) Register(rcvr any) error {
	svc := newService(rcvr)
	if _, dup := srv.serviceMap.LoadOrStore(svc.name, svc); dup {
		return errors.New("rpc: service already defined: " + svc.name)
	}
	return nil
}

// 查找服务
func (srv *Server) findService(method string) (*service, *methodType, error) {
	dot := strings.LastIndex(method, ".")
	if dot < 0 {
		return nil, nil, errors.New("rpc server: service/method request ill-formed: " + method)
	}
	serviceName, methodName := method[:dot], method[dot+1:]
	svci, ok := srv.serviceMap.Load(serviceName)
	if !ok {
		return nil, nil, errors.New("rpc server: can't find service " + serviceName)
	}
	svc := svci.(*service)
	mt := svc.methods[methodName]
	if mt == nil {
		return svc, mt, errors.New("rpc server: can't find method " + methodName)
	}
	return svc, mt, nil
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// 无效请求
var invalidRequest = struct{}{}

// 请求结构体
type request struct {
	header     *codec.Header
	argv, repv reflect.Value
	mtype      *methodType
	svc        *service
}

// 读取请求头
func (srv *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var header codec.Header
	err := cc.ReadHeader(&header)
	if err != nil {
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &header, nil
}

// 读取请求
func (srv *Server) readRequest(cc codec.Codec) (*request, error) {
	// 读取请求头
	header, err := srv.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	// 封装请求
	req := &request{header: header}
	req.svc, req.mtype, err = srv.findService(header.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.repv = req.mtype.newReplyv()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	err = cc.ReadBody(argvi)
	if err != nil {
		log.Println("rpc server: read argv error:", err)
		return req, err
	}
	return req, nil
}

// 发送响应信息
func (srv *Server) sendResponse(cc codec.Codec, header *codec.Header, body any, mutex *sync.Mutex) {
	mutex.Lock()
	defer mutex.Unlock()
	err := cc.Write(header, body)
	if err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 处理请求
func (srv *Server) handleRequest(cc codec.Codec, req *request, mutex *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.repv)
		called <- struct{}{}
		if err != nil {
			req.header.Error = err.Error()
			srv.sendResponse(cc, req.header, invalidRequest, mutex)
			sent <- struct{}{}
			return
		}
		srv.sendResponse(cc, req.header, req.repv.Interface(), mutex)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		req.header.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		srv.sendResponse(cc, req.header, invalidRequest, mutex)
	case <-called:
		<-sent
	}
}
