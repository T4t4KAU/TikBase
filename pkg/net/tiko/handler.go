package tiko

import (
	"errors"
	"fmt"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/queue"
	"github.com/T4t4KAU/TikBase/pkg/tlog"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"io"
)

type Handler struct {
	engine   iface.Engine
	commands map[byte]iface.INS
	channel  iface.Channel
	*tlog.Logger
}

func consumer(sub queue.Subscriber) {
	for msg := range sub {
		fmt.Println(utils.B2S(msg.Data.([]byte)))
	}
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
			_, err := writeReply(conn, Success, res.Data())
			if err != nil {
				_ = conn.Close()
				return
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
