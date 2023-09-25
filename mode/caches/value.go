package caches

import (
	"TikCache/pack/utils"
	"sync/atomic"
	"time"
)

const (
	NeverExpire = 0
)

const (
	INT = iota
	FLOAT
	STRING
	SET
	ZSET
	MAP
)

type value struct {
	Data    []byte // 数据
	TTL     int64  // 存活时间
	Created int64  // 数据创建时间
	Type    uint8
}

// 返回一个封装好的数据
func newValue(data []byte, ttl int64) *value {
	return &value{
		Data:    utils.Copy(data),
		TTL:     ttl,
		Created: time.Now().Unix(),
	}
}

// 返回该数据是否存活
func (v *value) alive() bool {
	return v.TTL == NeverExpire || time.Now().Unix()-v.Created < v.TTL
}

// 返回该数据实际存储数据
func (v *value) data() []byte {
	// 更新访问时间
	atomic.SwapInt64(&v.Created, time.Now().Unix())
	return v.Data
}
