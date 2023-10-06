package tiko

import (
	"TikBase/iface"
	"errors"
	"io"
)

type Handler struct {
	engine   iface.Engine
	commands map[byte]iface.INS
}

func NewHandler(eng iface.Engine) *Handler {
	handler := &Handler{
		engine:   eng,
		commands: make(map[byte]iface.INS),
	}
	handler.commands[getCommand] = iface.GET_STR
	handler.commands[setCommand] = iface.SET_STR
	handler.commands[deleteCommand] = iface.DEL
	handler.commands[expireCommand] = iface.EXPIRE
	return handler
}

func (h *Handler) Handle(conn iface.Connection) {
	ch := ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || isClosedError(payload.Err) {
				_ = conn.Close()
				return
			}
			_, err := writeErrorReply(conn, payload.Err.Error())
			if err != nil {
				_ = conn.Close()
				return
			}
			continue
		}

		// 获取对应指令
		ins, ok := h.commands[payload.Command]
		if !ok {
			_, err := writeErrorReply(conn, errCommandNotFound.Error())
			if err != nil {
				_ = conn.Close()
				return
			}
			continue
		}

		res := h.engine.Exec(ins, payload.Args)
		if res.Success() {
			n := len(res.Data())
			if n == 1 {
				_, err := writeReply(conn, Success, res.Data()[0])
				if err != nil {
					_ = conn.Close()
					return
				}
			} else if n > 1 {
				err := writeMultiReply(conn, Success, res.Data(), n)
				if err != nil {
					_ = conn.Close()
					return
				}
			} else {
				_, err := writeReply(conn, Success, nil)
				if err != nil {
					_ = conn.Close()
					return
				}
			}
		} else {
			_, err := writeErrorReply(conn, res.Error().Error())
			if err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}
