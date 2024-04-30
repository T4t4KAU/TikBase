package caches

import (
	"fmt"
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

func BenchmarkCache_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	c, err := New()
	assert.Nil(b, err)

	for i := 0; i < 100000; i++ {
		err := c.Set(fmt.Sprintf("key%d", i), []byte("value"), 0)
		assert.Nil(b, err)
	}
}

func BenchmarkCache_Get(b *testing.B) {
	c, err := New()
	assert.Nil(b, err)

	for i := 0; i < 100000; i++ {
		err := c.Set(fmt.Sprintf("key%d", i), []byte("value"), 0)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < 100000; i++ {
		_, err := c.Get(fmt.Sprintf("key%d", i))
		assert.Nil(b, err)
	}
}

func BenchmarkCache_Delete(b *testing.B) {
	c, err := New()
	assert.Nil(b, err)

	for i := 0; i < 100000; i++ {
		err := c.Set(fmt.Sprintf("key%d", i), []byte("value"), 0)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < 100000; i++ {
		err := c.Del(fmt.Sprintf("key%d", i))
		assert.Nil(b, err)
	}
}
