package caches

import (
	"fmt"
	"testing"
)

func TestCache_Set(t *testing.T) {
	c := NewCache()
	err := c.Set("key1", []byte("value"), STRING)
	if err != nil {
		t.Error(err.Error())
	}
	v, _ := c.Get("key1")
	fmt.Printf("%v\n", v.String())

	err = c.SetInt("key2", 100, 0)
	if err != nil {
		t.Error(err.Error())
	}
	v, _ = c.Get("key2")
	fmt.Printf("%v\n", v.String())
}
