package values

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"strconv"
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

// New 返回一个封装好的数据
func New(data []byte, ttl int64, typ iface.Type) Value {
	return Value{
		Data:    utils.Copy(data),
		TTL:     ttl,
		Created: time.Now().Unix(),
		Type:    typ,
	}
}

func NewMeta(data []byte) Value {
	return Value{
		Data:    data,
		TTL:     0,
		Created: time.Now().Unix(),
		Type:    iface.META_DATA,
	}
}

func MewString(data []byte, ttl int64) Value {
	return Value{
		Data:    data,
		TTL:     ttl,
		Created: time.Now().Unix(),
		Type:    iface.STRING,
	}
}

func NewHash(data []byte, ttl int64) Value {
	return Value{
		Data:    data,
		TTL:     ttl,
		Created: time.Now().Unix(),
		Type:    iface.HASH,
	}
}

func NewSet(data []byte, ttl int64) Value {
	return Value{
		Data:    data,
		TTL:     ttl,
		Created: time.Now().Unix(),
		Type:    iface.SET,
	}
}

func (v *Value) Score() float64 {
	val, _ := strconv.ParseFloat(utils.B2S(v.data()), 64)
	return val
}

func (v *Value) String() string {
	switch v.Type {
	case iface.STRING:
		return v.toString()
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

func (v *Value) Time() int64 {
	return v.TTL
}

func (v *Value) toString() string {
	return string(v.data())
}

// Alive 返回该数据是否存活
func (v *Value) Alive() bool {
	return v.TTL == NeverExpire || time.Now().Unix()-v.Created < v.TTL
}

// 返回该数据实际存储数据
func (v *Value) data() []byte {
	// 更新访问时间
	atomic.SwapInt64(&v.Created, time.Now().Unix())
	return v.Data
}
