package tiko

import (
	"TikBase/iface"
	"TikBase/pack/queue"
	"TikBase/pack/tlog"
	"TikBase/pack/utils"
	"errors"
	"fmt"
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
		// channel:  logq.New("tiko handler", consumer),
	}

	// 暂时不输出日志
	//  handler.Logger = tlog.New(tlog.WithLevel(tlog.InfoLevel),
	//	tlog.WithOutput(handler.channel),
	//	tlog.WithFormatter(&tlog.TextFormatter{IgnoreBasicFields: false}),
	//	tlog.WithLevel(tlog.WarnLevel),
	//)

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
			// h.Info("handle error:", errCommandNotFound.Error())
			_, err := writeErrorReply(conn, errCommandNotFound.Error())
			if err != nil {
				_ = conn.Close()
				return
			}
			continue
		}

		res := h.engine.Exec(ins, payload.Args)
		// h.Info(ins.String(), " command args: ", payload.Args, " result: ", res.Success())

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
