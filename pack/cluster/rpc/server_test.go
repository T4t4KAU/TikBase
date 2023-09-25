package rpc

import (
	"context"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

func startServer(addr chan string) {
	var b Bar
	Register(&b)
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalln("network error: ", err)
	}
	log.Println("start rpc server on", lis.Addr())
	addr <- lis.Addr().String()
	Accept(lis)
}

type Bar int

func (Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 2)
	return nil
}

func (Bar) Square(argv int, reply *int) error {
	*reply = argv * argv
	return nil
}

type AddReq struct {
	A int
	B int
}

type AddReply struct {
	Sum int
}

func (Bar) Add(req AddReq, reply *AddReply) {
	reply.Sum = req.A + req.B
}

func TestClientDialTimeout(t *testing.T) {
	t.Parallel()
	lis, _ := net.Listen("tcp", ":0")
	f := func(conn net.Conn, opt *Option) (client *Client, err error) {
		conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", lis.Addr().String(), &Option{ConnectTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connection timeout"), "expect a timeout error")
	})
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", lis.Addr().String(), &Option{ConnectTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}

func TestClientCall(t *testing.T) {
	t.Parallel()
	addrCh := make(chan string)
	go startServer(addrCh)
	addr := <-addrCh
	time.Sleep(time.Second)

	t.Run("test square", func(t *testing.T) {
		cli, _ := Dial("tcp", addr)
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply, num int
		num = 2
		err := cli.Call(ctx, "Bar.Square", num, &reply)
		_assert(err == nil && reply == num*num, "call failed")
	})

	t.Run("test add", func(t *testing.T) {
		cli, _ := Dial("tcp", addr)
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply AddReply
		err := cli.Call(ctx, "Bar.Add", AddReq{1, 2}, &reply)
		_assert(err == nil && reply.Sum == 3, "call failed")
	})

	t.Run("client timeout", func(t *testing.T) {
		cli, _ := Dial("tcp", addr)
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply int
		err := cli.Call(ctx, "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")
	})
	t.Run("server handle timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr, &Option{
			HandleTimeout: time.Second,
		})
		var reply int
		err := client.Call(context.Background(), "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), "handle timeout"), "expect a timeout error")
	})
}
