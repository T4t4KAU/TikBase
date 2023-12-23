package poll

import (
	"github.com/T4t4KAU/TikBase/iface"
	"net"
	"strconv"
	"testing"
	"time"
)

type testHandler struct {
}

func (h *testHandler) Handle(conn iface.Connection) {
	b := make([]byte, 1024)
	_, _ = conn.Read(b)
	println(string(b))
	_, _ = conn.Write([]byte("Hello " + string(b)))
}

func (h *testHandler) Close() error {
	//TODO implement me
	panic("implement me")
}

func startNetPoll() {
	config := Config{
		Address:    "127.0.0.1:9999",
		MaxConnect: 20,
		Timeout:    10 * time.Second,
	}
	p := New(config, &testHandler{})
	err := p.Run()
	if err != nil {
		panic(err)
	}
}

func TestNetPoll_Run(t *testing.T) {
	go startNetPoll()
	time.Sleep(time.Second)

	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:9999")
		if err != nil {
			t.Error(err.Error())
			return
		}
		_, _ = conn.Write([]byte(strconv.FormatInt(int64(i), 10)))

		b := make([]byte, 1024)
		_, _ = conn.Read(b)
		println(string(b))
	}
}
