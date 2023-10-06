package caches

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"testing"
	"time"
)

func TestCache_Set(t *testing.T) {
	c := New()
	v := values.New([]byte("value"), 0, iface.STRING)
	c.Set("key", v)
	res, _ := c.Get("key")
	println(res.String())
}

func TestCache_Expire(t *testing.T) {
	c := New()
	v := values.New([]byte("value"), 0, iface.STRING)
	c.Set("key", v)
	res, _ := c.Get("key")
	println(res.String())
	ok := c.Expire("key", 1)
	if !ok {
		println(ok)
		return
	}
	time.Sleep(time.Second)
	res, _ = c.Get("key")
	println(res.Alive())
}
