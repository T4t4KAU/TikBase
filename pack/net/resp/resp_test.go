package resp

import (
	"bytes"
	"fmt"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pack/poll"
	"github.com/T4t4KAU/TikBase/pack/utils"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"testing"
	"time"
)

func TestParseStream1(t *testing.T) {
	replies := []iface.Reply{
		MakeIntReply(1),
		MakeStatusReply("OK"),
		MakeErrReply("ERR unknown"),
		MakeBulkReply([]byte("a\r\nb")), // test binary safe
		MakeNullBulkReply(),
		MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		MakeEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + CRLF)) // test text protocol
	expected := make([]iface.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}

func TestParseStream2(t *testing.T) {
	reqs := bytes.Buffer{}
	reqs.Write([]byte("*3\r\n" + "$3\r\nSET\r\n" + "$3\r\nkey\r\n" + "$5\r\n" + "value\r\n")) // test text protocol

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		} else {
			fmt.Print(string(payload.Data.ToBytes()))
		}
	}
}

func startServer() {
	eng, _ := engine.NewBaseEngine()
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

func TestClient(t *testing.T) {
	go startServer()
	time.Sleep(time.Second)

	cli, err := NewClient("127.0.0.1:9999")
	err = cli.Set("key", "value")
	assert.Nil(t, err)

	val, err := cli.Get("key")
	assert.Nil(t, err)
	assert.Equal(t, "value", val)

	err = cli.HSet("hash", "key1", "value1")
	assert.Nil(t, err)

	val, err = cli.HGet("hash", "key1")
	assert.Nil(t, err)
	assert.Equal(t, "value1", val)
}

func TestParseStream3(t *testing.T) {
	go startServer()
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Error(err.Error())
		return
	}
	_, _ = conn.Write([]byte("set key value\r\n"))
	b := make([]byte, 1024)
	_, _ = conn.Read(b)
	println(string(b))
	_, _ = conn.Write([]byte("get key\r\n"))
	_, _ = conn.Read(b)
	println(string(b))
}
