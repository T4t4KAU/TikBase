package tcp

import (
	"TikCache/engine/caches"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"sync"
)

type Server struct {
	listener net.Listener
	handlers map[byte]func(args [][]byte) ([]byte, error)
	cache    *caches.Cache
}

// NewServer 创建TCP服务器
func NewServer(c *caches.Cache) *Server {
	return &Server{
		handlers: make(map[byte]func(args [][]byte) ([]byte, error)),
		cache:    c,
	}
}

// Run 启动服务器
func (s *Server) Run(address string) error {
	// 注册处理函数
	s.RegisterHandler(getCommand, s.getHandler)
	s.RegisterHandler(setCommand, s.setHandler)
	s.RegisterHandler(deleteCommand, s.deleteHandler)
	s.RegisterHandler(statusCommand, s.statusHandler)
	return s.ListenAndServe("tcp", address)
}

// RegisterHandler 注册命令处理器
func (s *Server) RegisterHandler(command byte, handler func(args [][]byte) ([]byte, error)) {
	s.handlers[command] = handler
}

// ListenAndServe 监听并处理连接
func (s *Server) ListenAndServe(network string, address string) error {
	var err error
	s.listener, err = net.Listen(network, address)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if checkNetworkError(err) {
				break
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 处理连接
			s.handleConn(conn)
		}()
	}

	wg.Wait()
	return nil
}

// handleConn 处理连接
func (s *Server) handleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	defer conn.Close()
	for {
		// 读取并解析请求
		command, args, err := readRequestFrom(reader)
		if err != nil {
			if errors.Is(err, errProtocolVersionMismatch) {
				continue
			}
			return
		}

		// 处理请求
		reply, body, err := s.handleRequest(command, args)
		if err != nil {
			_, _ = writeErrorResponseTo(conn, err.Error())
			continue
		}

		// 发送处理结果
		_, err = writeResponseTo(conn, reply, body)
		if err != nil {
			continue
		}
	}
}

// 处理请求
func (s *Server) handleRequest(command byte, args [][]byte) (byte, []byte, error) {
	handle, ok := s.handlers[command] // 获取对应处理函数
	if !ok {
		return ErrorResp, nil, errCommandHandlerNotFound
	}

	// 将处理结果返回
	body, err := handle(args)
	if err != nil {
		return ErrorResp, body, err
	}
	return SuccessResp, body, err
}

// 处理get指令
func (s *Server) getHandler(args [][]byte) (body []byte, err error) {
	if len(args) < 1 {
		return nil, errCommandNeedsMoreArguments
	}

	// 调用缓存Get方法 如果不存在则返回NotFound错误
	value, ok := s.cache.Get(string(args[0]))
	if !ok {
		return value, errNotFound
	}
	return value, nil
}

// 处理set指令
func (s *Server) setHandler(args [][]byte) (body []byte, err error) {
	if len(args) < 3 {
		return nil, errCommandNeedsMoreArguments
	}

	// 读取TTL 使用大端方式读取 客户端同样使用大端方式存储
	ttl := int64(binary.BigEndian.Uint64(args[0]))
	err = s.cache.SetWithTTL(string(args[1]), args[2], ttl)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// 处理delete指令
func (s *Server) deleteHandler(args [][]byte) (body []byte, err error) {
	if len(args) < 1 {
		return nil, errCommandNeedsMoreArguments
	}
	err = s.cache.Delete(string(args[0]))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// 处理status指令
func (s *Server) statusHandler(args [][]byte) (body []byte, err error) {
	return json.Marshal(s.cache.Status())
}

// Close 关闭服务端的方法
func (s *Server) Close() error {
	if s.listener == nil {
		return nil
	}
	return s.listener.Close()
}
