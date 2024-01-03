package engine

import (
	"testing"

	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/stretchr/testify/assert"
)

func TestCacheEngine_Exec(t *testing.T) {
	e, _ := NewCacheEngine()
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

func TestBaseEngine_Exec(t *testing.T) {
	e, _ := NewBaseEngine()
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

func TestBaseEngine_ExecHashSet(t *testing.T) {
	e, _ := NewBaseEngine()
	res := e.Exec(iface.SET_HASH, [][]byte{[]byte("hash"), []byte("key1"), []byte("value1")})
	assert.Nil(t, res.Error())

	res = e.Exec(iface.GET_HASH, [][]byte{[]byte("hash"), []byte("key1")})
	assert.Nil(t, res.Error())
	assert.Equal(t, []byte("value1"), res.Data())
	println(string(res.Data()))
}

func TestBaseEngine_ExecListPush(t *testing.T) {
	e, _ := NewBaseEngine()
	res := e.Exec(iface.LEFT_POP_LIST, [][]byte{[]byte("list"), []byte("element1")})
	assert.Nil(t, res.Error())

	res = e.Exec(iface.RIGHT_POP_LIST, [][]byte{[]byte("list")})
	assert.Nil(t, res.Error())
	assert.Equal(t, []byte("element1"), res.Data())
	println(string(res.Data()))
}

func TestBaseEngine_ExecSetAdd(t *testing.T) {
	e, _ := NewBaseEngine()
	res := e.Exec(iface.ADD_SET, [][]byte{[]byte("set"), []byte("element1")})
	assert.Nil(t, res.Error())

	res = e.Exec(iface.ADD_SET, [][]byte{[]byte("set"), []byte("element2")})
	assert.Nil(t, res.Error())

	res = e.Exec(iface.IS_MEMBER_SET, [][]byte{[]byte("set"), []byte("element1")})
	assert.Nil(t, res.Error())
	assert.True(t, res.Success())

	res = e.Exec(iface.REM_SET, [][]byte{[]byte("set"), []byte("element1")})
	assert.Nil(t, res.Error())

	res = e.Exec(iface.IS_MEMBER_SET, [][]byte{[]byte("set"), []byte("element1")})
	assert.Equal(t, errno.ErrSetMemberNotFound, res.Error())
	assert.False(t, res.Success())
}
