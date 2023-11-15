package tiko

import (
	"TikBase/engine"
	"TikBase/pack/poll"
	"bytes"
	"net"
	"testing"
	"time"
)

func startServer() {
	eng := engine.NewCacheEngine()
	p := poll.New(poll.Config{
		Address:    "127.0.0.1:9999",
		MaxConnect: 20,
		Timeout:    time.Second,
	}, NewHandler(eng))
	err := p.Run()
	if err != nil {
		panic(err)
	}
}

func TestParseStream(t *testing.T) {
	go startServer()
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = writeSetRequest(conn, []byte("key"), []byte("value"))
	if err != nil {
		t.Error(err.Error())
		return
	}
	code, data, err := parseReply(conn)
	println(int(code), string(data))

	_, err = writeGetRequest(conn, []byte("key"))
	if err != nil {
		t.Error(err.Error())
		return
	}

	code, data, err = parseReply(conn)
	if err != nil {
		t.Error(err.Error())
		return
	}
	println(int(code), string(data))

	_, err = writeDelRequest(conn, []byte("key"))
	if err != nil {
		t.Error(err.Error())
		return
	}

	code, data, err = parseReply(conn)
	if err != nil {
		t.Error(err.Error())
		return
	}
	println(int(code), string(data))

	_, err = writeGetRequest(conn, []byte("key"))
	if err != nil {
		t.Error(err.Error())
		return
	}

	code, data, err = parseReply(conn)
	if err != nil {
		t.Error(err.Error())
		return
	}
	println(int(code), string(data))
}

func TestGetRequest_Bytes(t *testing.T) {
	var buff bytes.Buffer

	req := MakeGetRequest("key")
	buff.Write(req.Bytes())
	command, args, err := parseRequest(&buff)
	if err != nil {
		t.Error(err.Error())
		return
	}

	println(int(command), string(args[0]))
}

func TestSetRequest_Bytes(t *testing.T) {
	var buff bytes.Buffer

	req := MakeSetRequest("key", "value")
	buff.Write(req.Bytes())
	command, args, err := parseRequest(&buff)
	if err != nil {
		t.Error(err.Error())
		return
	}

	println(int(command), string(args[0]), string(args[1]))
}
