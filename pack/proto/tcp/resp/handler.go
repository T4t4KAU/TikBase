package resp

import (
	"TikCache/iface"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

type Boolean uint32

// Get reads the value atomically
func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

// Set writes the value atomically
func (b *Boolean) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}

type Handler struct {
	activeConn sync.Map
	engine     iface.Engine
	closing    Boolean
}

func NewHandler(eng iface.Engine) *Handler {
	return &Handler{
		engine: eng,
	}
}

// Handle 请求处理
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
	}

	client := NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	ch := ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || isClosedError(payload.Err) {
				h.closeClient(client)
				return
			}
			errReply := MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				return
			}
			continue
		}

		if payload.Data == nil {
			continue
		}

		// TODO: Execute command
	}
}

func (h *Handler) Close() error {
	h.closing.Set(true)
	h.activeConn.Range(func(key any, value any) bool {
		c := key.(iface.Connection)
		c.Close()
		return true
	})
	h.engine.Close()
	return nil
}

func (h *Handler) closeClient(cli iface.Connection) {
	cli.Close()
	h.activeConn.Delete(cli)
}

func isClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
