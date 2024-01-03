package test

import (
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	http2 "github.com/T4t4KAU/TikBase/pkg/net/http"
	"github.com/T4t4KAU/TikBase/pkg/net/resp"
	"github.com/T4t4KAU/TikBase/pkg/net/tiko"
	"github.com/T4t4KAU/TikBase/pkg/poll"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	keySize = 100000 // 测试键值对数量
)

// 封装测试任务
func testTask(task func(no int)) string {
	beginTime := time.Now()
	for i := 0; i < keySize; i++ {
		task(i)
	}
	return time.Since(beginTime).String()
}

func startHTTPServer() {
	eng, _ := engine.NewCacheEngine()
	s := http2.NewServer(eng)
	err := s.Run(":9999")
	if err != nil {
		panic(err)
	}
}

// 测试HTTP接口
func TestHTTPServer(t *testing.T) {
	go startHTTPServer()
	time.Sleep(time.Second)

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		request, err := http.NewRequest("PUT",
			"http://localhost:9999/cache/"+data, strings.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	})
	t.Logf("consume write time: %s\n", writeTime)
	time.Sleep(3 * time.Second)
	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		request, err := http.NewRequest("GET",
			"http://localhost:9999/cache/"+data, strings.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	})
	t.Logf("consume read time: %s\n", readTime)
}

func startServer(proto, engName string) {
	var handler iface.Handler

	eng, _ := engine.NewEngine(engName)

	if proto == "resp" {
		handler = resp.NewHandler(eng)
	}
	if proto == "tiko" {
		handler = tiko.NewHandler(eng)
	}

	p := poll.New(poll.Config{
		Address:    "127.0.0.1:9999",
		MaxConnect: 1000,
		Timeout:    time.Second,
	}, handler)
	err := p.Run()
	if err != nil {
		panic(err)
	}
}

func TestTikoServer(t *testing.T) {
	go startServer("tiko", "cache")
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Error(err.Error())
		return
	}

	client := tiko.NewClient(conn)
	if err != nil {
		t.Fatal(err)
	}

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		err = client.Set(data, data)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("consume write time: %s\n", writeTime)
	time.Sleep(3 * time.Second)
	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		res, err := client.Get(data)
		if err != nil || res != data {
			t.Fatal(err)
		}
	})
	t.Logf("consume read time: %s\n", readTime)
}

func TestRespServer(t *testing.T) {
	go startServer("resp", "cache")
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Error(err.Error())
		return
	}

	cli := resp.NewClient(conn)
	if err != nil {
		t.Fatal(err)
	}

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		err = cli.Set(data, data)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("consume write time: %s\n", writeTime)
	time.Sleep(3 * time.Second)
	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		res, err := cli.Get(data)
		if err != nil || res != data {
			t.Fatal(err)
		}
	})
	t.Logf("consume read time: %s\n", readTime)
}
