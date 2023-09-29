package levels

import (
	"TikCache/engine/values"
	"TikCache/pack/iface"
	"testing"
)

func TestLevels_Get(t *testing.T) {
	c := New()
	v := values.New([]byte("value1"), 0, iface.STRING)
	c.Set("key", v)
	res, _ := c.Get("key")
	println(res.String())
	v = values.New([]byte("value2"), 0, iface.STRING)
	c.Set("key", v)
	res, _ = c.Get("key")
	println(res.String())
}
