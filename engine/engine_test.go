package engine

import (
	"TikBase/iface"
	"testing"
)

func TestCacheEngine_Exec(t *testing.T) {
	e := NewCacheEngine()
	res := e.Exec(iface.SET_STR, [][]byte{[]byte("key1"), []byte("value1")})
	if !res.Success() {
		return
	}
	res = e.Exec(iface.GET_STR, [][]byte{[]byte("key1")})
	if !res.Success() {
		return
	}
	println(string(res.Data()[0]))
}

func TestLevelEngine_Exec(t *testing.T) {
	e := NewLevelEngine()
	res := e.Exec(iface.SET_STR, [][]byte{[]byte("key1"), []byte("value1")})
	if !res.Success() {
		return
	}
	res = e.Exec(iface.GET_STR, [][]byte{[]byte("key1")})
	if !res.Success() {
		return
	}
	println(string(res.Data()[0]))
}
