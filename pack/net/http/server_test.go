package http

import (
	"TikBase/engine"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"testing"
	"time"
)

func SendSetReq(key, value string) (int, []byte, error) {
	url := "http://127.0.0.1:9096/cache/" + key
	payload := []byte(value)

	request, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println("创建请求失败:", err)
		return 0, nil, err
	}

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		fmt.Println("发送请求失败:", err)
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func SendGetReq(key string) (int, []byte, error) {
	// 发起 HTTP GET 请求
	resp, err := http.Get("http://127.0.0.1:9096/cache/" + key)
	if err != nil {
		fmt.Println("request failed:", err)
		return 0, nil, err
	}
	defer resp.Body.Close()

	// 读取响应内容
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("read response:", err)
		return 0, nil, err
	}

	return resp.StatusCode, body, nil
}

func startServer() error {
	eng, _ := engine.NewCacheEngine()
	s := NewServer(eng)
	err := s.Run(":9096")
	if err != nil {
		log.Fatalln("server start failed:", err)
		return err
	}
	return nil
}

func TestServer_Echo(t *testing.T) {
	go func() {
		err := startServer()
		if err != nil {
			t.Error(err.Error())
		}
	}()

	// 发起 HTTP GET 请求
	resp, err := http.Get("http://127.0.0.1:9096/echo/hello")
	if err != nil {
		fmt.Println("request failed:", err)
		return
	}
	defer resp.Body.Close()

	// 读取响应内容
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("read response:", err)
		return
	}

	// 打印响应内容
	fmt.Println(string(body))
}

func TestServer_Run(t *testing.T) {
	go func() {
		err := startServer()
		if err != nil {
			t.Error(err.Error())
		}
	}()

	time.Sleep(time.Second)

	code, data, err := SendSetReq("key", "value")
	if err != nil {
		t.Error(err.Error())
	}
	println(code, string(data))
	code, data, err = SendGetReq("key")
	if err != nil {
		t.Error()
	}
	println(code, string(data))
}
