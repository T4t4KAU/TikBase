package poll

import (
	"context"
)

type OnPrepare func(conn Connection) context.Context

type OnConnect func(ctx context.Context, conn Connection)

type OnRequest func(ctx context.Context, connection Connection) error

type EventLoop struct {
	stop bool
}

func NewEventLoop() *EventLoop {
	return &EventLoop{}
}

func (e *EventLoop) Run() {

}

func ProcessEvents(e *EventLoop) {

}
