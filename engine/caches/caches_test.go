package caches

import (
	"github.com/T4t4KAU/TikBase/engine/values"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCache_Set(t *testing.T) {
	c, _ := New()
	v := values.New([]byte("value"), 0, iface.STRING)
	err := c.Set("key", &v)
	assert.Nil(t, err)
	res, _ := c.Get("key")
	println(res.String())
}

func TestCache_Expire(t *testing.T) {
	c, _ := New()
	v := values.New([]byte("value"), 0, iface.STRING)
	err := c.Set("key", &v)
	assert.Nil(t, err)

	res, _ := c.Get("key")
	println(res.String())

	err = c.Expire("key", 1)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	res, _ = c.Get("key")
	println(res.Alive())
}
