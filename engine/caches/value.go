package caches

import (
	"TikCache/iface"
	"TikCache/pack/utils"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	NeverExpire = 0
)

type Value struct {
	Data    []byte     // 数据
	TTL     int64      // 存活时间
	Created int64      // 数据创建时间
	Type    iface.Type // 数据类型
}

// 返回一个封装好的数据
func newValue(data []byte, ttl int64, typ iface.Type) *Value {
	return &Value{
		Data:    utils.Copy(data),
		TTL:     ttl,
		Created: time.Now().Unix(),
		Type:    typ,
	}
}

func (v *Value) Score() float32 {
	return 0
}

func (v *Value) String() string {
	switch v.Type {
	case iface.STRING:
		return v.toString()
	case iface.INT:
		return fmt.Sprintf("%d", v.toInt())
	default:
		panic("wrong type")
	}
}

func (v *Value) Bytes() []byte {
	return v.data()
}

func (v *Value) Attr() iface.Type {
	return v.Type
}

func (v *Value) toInt() int {
	return utils.BytesToInt(v.data())
}

func (v *Value) toString() string {
	return string(v.data())
}

// 返回该数据是否存活
func (v *Value) alive() bool {
	return v.TTL == NeverExpire || time.Now().Unix()-v.Created < v.TTL
}

// 返回该数据实际存储数据
func (v *Value) data() []byte {
	// 更新访问时间
	atomic.SwapInt64(&v.Created, time.Now().Unix())
	return v.Data
}
