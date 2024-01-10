package resp

import (
	"errors"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"io"
	"strings"
)

type Handler struct {
	engine   iface.Engine
	keywords map[string]iface.INS
}

func (h *Handler) BulkToIns(arg []byte) (iface.INS, bool) {
	keyword := utils.B2S(arg)
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
	h.keywords["expire"] = iface.EXPIRE

	h.keywords["hget"] = iface.GET_HASH
	h.keywords["hset"] = iface.SET_HASH
	h.keywords["hdel"] = iface.DEL_HASH

	h.keywords["lpush"] = iface.LEFT_PUSH_LIST
	h.keywords["lpop"] = iface.LEFT_POP_LIST

	return h
}

func (h *Handler) handleReply(reply *MultiBulkReply, conn iface.Connection) (err error) {
	if ins, ok := h.BulkToIns(reply.Args[0]); ok {
		res := h.engine.Exec(ins, reply.Args[1:])
		if res.Success() {
			if len(res.Data()) > 0 {
				_, err = conn.Write(MakeBulkReply(res.Data()).ToBytes())
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
