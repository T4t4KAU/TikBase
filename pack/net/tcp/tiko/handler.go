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
			if len(res.Data()) > 0 {
				_, _ = writeReply(conn, Success, res.Data()[0])
			} else {
				_, _ = writeReply(conn, Success, nil)
			}
		} else {
			_, _ = writeErrorReply(conn, res.Error().Error())
		}
	}
}
