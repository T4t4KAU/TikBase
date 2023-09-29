package caches

import (
	"TikCache/engine/values"
	"TikCache/pack/iface"

	"testing"
)

func TestCache_Set(t *testing.T) {
	c := New()
	v := values.New([]byte("value"), 0, iface.STRING)
	c.Set("key", v)
	res, _ := c.Get("key")
	println(res.String())
}
