package resp

import (
	"bytes"
	"fmt"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/pack/poll"
	"io"
	"net"
	"testing"
	"time"
)

func TestParseStream1(t *testing.T) {
	reqs := bytes.Buffer{}
	reqs.Write([]byte("set a a" + CRLF)) // test text protocol

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
	eng, _ := engine.NewCacheEngine()
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

func TestWriteGetRequest(t *testing.T) {
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
	b := make([]byte, 1024)
	_, _ = conn.Read(b)
	println(string(b))

	_, err = writeGetRequest(conn, []byte("key"))
	if err != nil {
		t.Error(err.Error())
		return
	}
	b = make([]byte, 1024)
	_, _ = conn.Read(b)
	println(string(b))
}

func TestClient(t *testing.T) {
	go startServer()
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Error(err.Error())
		return
	}
	cli := NewClient(conn)
	val, err := cli.Set("key", "value")
	if err != nil {
		t.Error(err.Error())
		return
	}
	println(val)

	val, err = cli.Get("key")
	if err != nil {
		t.Error(err.Error())
		return
	}
	println(val)
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

func TestReadMultiBulk(t *testing.T) {
	reqs := bytes.Buffer{}
	reqs.Write([]byte("*3\r\n" + "$3\r\nSET\r\n" + "$3\r\nkey\r\n" + "$5\r\n" + "value\r\n")) // test text protocol
	payloads := readLine(&reqs)

	for _, payload := range payloads {
		if payload.Err == nil {
			println(string(payload.Data.ToBytes()))
		} else {
			println(payload.Err.Error())
		}
	}
}
