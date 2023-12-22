package resp

import (
	"TikBase/iface"
	"errors"
	"io"
	"strings"
)

type Handler struct {
	engine   iface.Engine
	keywords map[string]iface.INS
}

func (h *Handler) BulkToIns(arg []byte) (iface.INS, bool) {
	keyword := string(arg)
	keyword = strings.ToLower(keyword)
	ins, ok := h.keywords[keyword]
	if !ok {
		return iface.NIL, false
	}
	return ins, true
}

func NewHandler(eng iface.Engine) *Handler {
	h := &Handler{
		engine:   eng,
		keywords: make(map[string]iface.INS),
	}
	h.keywords["set"] = iface.SET_STR
	h.keywords["get"] = iface.GET_STR
	h.keywords["del"] = iface.DEL
	return h
}

func (h *Handler) handleReply(reply *MultiBulkReply, conn iface.Connection) (err error) {
	if ins, ok := h.BulkToIns(reply.Args[0]); ok {
		res := h.engine.Exec(ins, reply.Args[1:])
		if res.Success() {
			if len(res.Data()) > 0 {
				_, err = conn.Write(MakeBulkReply(res.Data()[0]).ToBytes())
				if err != nil {
					_ = conn.Close()
				}
			} else {
				_, err = conn.Write(MakeOkReply().ToBytes())
				if err != nil {
					_ = conn.Close()
				}
			}
		} else {
			_, err = conn.Write(MakeErrReply(res.Error().Error()).ToBytes())
			if err != nil {
				_ = conn.Close()
			}
		}
	} else {
		_, err = conn.Write(MakeUnknownCommandErrReply(reply.Args[0]).ToBytes())
		if err != nil {
			_ = conn.Close()
		}
	}
	return nil
}

// Handle 请求处理
func (h *Handler) Handle(conn iface.Connection) {
	ch := ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || isClosedError(payload.Err) {
				_ = conn.Close()
				return
			}
			errReply := MakeErrReply(payload.Err.Error())
			_, err := conn.Write(errReply.ToBytes())
			if err != nil {
				_ = conn.Close()
				return
			}
			continue
		}

		if payload.Data == nil {
			continue
		}

		// 接收到命令
		reply, ok := (payload).Data.(*MultiBulkReply)
		if !ok {
			continue
		}
		err := h.handleReply(reply, conn)
		if err != nil {
			return
		}
	}
}

func isClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}