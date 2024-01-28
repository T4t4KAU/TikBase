package caches

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCache_Set(t *testing.T) {
	c, _ := New()
	err := c.Set("key", []byte("value"), 0)
	assert.Nil(t, err)
	res, _ := c.Get("key")
	println(res.String())
}

func TestCache_Expire(t *testing.T) {
	c, _ := New()
	err := c.Set("key", []byte("value"), 0)
	assert.Nil(t, err)

	res, _ := c.Get("key")
	println(res.String())

	err = c.Expire("key", 1)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	res, _ = c.Get("key")
	println(res.Alive())
}

func TestDict_Get(t *testing.T) {
	dict := NewDict()
	assert.NotEqual(t, dict, nil)

	ok := dict.Put("key1", "123")
	assert.True(t, ok)
	val, ok := dict.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, val, "123")
}

func TestSet_Add(t *testing.T) {
	set := NewSet()
	assert.NotEqual(t, set, nil)

	ok := set.Add("elem1")
	assert.True(t, ok)

	ok = set.Has("elem1")
	assert.True(t, ok)

	set.Remove("elem1")
	ok = set.Has("elem1")
	assert.False(t, ok)
}
