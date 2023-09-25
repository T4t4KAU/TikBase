package test

import (
	"TikCache/net/tcp"
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

// 测试HTTP接口
func TestHTTPServer(t *testing.T) {
	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		request, err := http.NewRequest("PUT",
			"http://localhost:9960/v1/cache/"+data, strings.NewReader(data))
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
			"http://localhost:9960/v1/cache/"+data, strings.NewReader(data))
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

func TestTCPServer(t *testing.T) {
	client, err := tcp.NewClient(":9960")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		err := client.Set(data, []byte(data), 0)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("consume write time: %s\n", writeTime)
	time.Sleep(3 * time.Second)
	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		_, err := client.Get(data)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("consume read time: %s\n", readTime)
}

func TestAsyncClientPerformance(t *testing.T) {
	c, err := tcp.NewAsyncClient(":9960")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Exit()

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		c.Set(data, []byte(data), 0)
	})
	t.Logf("consume write time: %s\n", writeTime)
	time.Sleep(3 * time.Second)

	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		c.Get(data)
	})
	t.Logf("consume read time: %s\n", readTime)
	time.Sleep(time.Second)
}
