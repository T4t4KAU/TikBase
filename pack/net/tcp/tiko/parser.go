package tiko

import (
	"TikBase/pack/tlog"
	"TikBase/pack/utils"
	"io"
	"runtime/debug"
)

type Payload struct {
	Command byte
	Args    [][]byte
	Err     error
}

func ParseStream(reader io.Reader) chan *Payload {
	ch := make(chan *Payload, 1)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan *Payload) {
	defer func() {
		if err := recover(); err != nil {
			tlog.Error(err, utils.BytesToString(debug.Stack()))
		}
	}()

	for {
		command, args, err := parseRequest(reader)
		if err != nil {
			ch <- &Payload{Err: err}
			continue
		}
		ch <- &Payload{
			Command: command,
			Args:    args,
			Err:     err,
		}
	}
}
