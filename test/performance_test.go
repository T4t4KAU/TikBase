package test

import (
	"TikBase/engine"
	"TikBase/pack/net/tcp/tiko"
	"TikBase/pack/poll"
	"github.com/go-redis/redis"
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

func startServer() {
	eng := engine.NewCacheEngine()
	p := poll.New(&poll.Config{
		Address:    "127.0.0.1:9999",
		MaxConnect: 20,
		Timeout:    time.Second,
	}, tiko.NewHandler(eng))
	err := p.Run()
	if err != nil {
		panic(err)
	}
}

func TestTCPServer(t *testing.T) {
	go startServer()
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
		_, err = client.Get(data)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("consume read time: %s\n", readTime)
}

func TestRedisServer(t *testing.T) {
	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis 服务器地址和端口
		Password: "",               // Redis 服务器密码
		DB:       0,                // Redis 数据库索引
	})

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		err := client.Set(data, data, 0).Err()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("consume write time: %s\n", writeTime)
	time.Sleep(3 * time.Second)
	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		_, err := client.Get(data).Result()
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("consume read time: %s\n", readTime)
}
