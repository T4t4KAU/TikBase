package rpc

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

type Bar int

type AddReq struct {
	A int
	B int
}

type AddRep struct {
	Sum int
}

func (b Bar) Add(req AddReq, reply *AddRep) error {
	reply.Sum = req.A + req.B
	return nil
}

func (b Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 2)
	return nil
}

func startServer() {
	var b Bar
	_ = Register(&b)
	// pick a free port
	l, _ := net.Listen("tcp", ":8999")
	Accept(l)
}

func TestClient_Call(t *testing.T) {
	go startServer()
	time.Sleep(time.Second)

	client, err := Dial("tcp", ":8999")
	assert.Nil(t, err)

	ctx := context.Background()

	var reply AddRep
	err = client.Call(ctx, "Bar.Add", AddReq{1, 2}, &reply)
	assert.Nil(t, err)
	assert.Equal(t, 3, reply.Sum)

	err = client.Call(ctx, "Bar.Add", AddReq{2, 5}, &reply)
	assert.Nil(t, err)
	assert.Equal(t, 7, reply.Sum)

	err = client.Call(ctx, "Bar.Add", AddReq{100, 206}, &reply)
	assert.Nil(t, err)
	assert.Equal(t, 306, reply.Sum)

	err = client.Call(ctx, "Bar.Add", AddReq{1, 2}, &reply)
	assert.Nil(t, err)
	assert.Equal(t, 3, reply.Sum)
}
