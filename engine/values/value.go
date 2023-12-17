package values

import (
	"TikBase/iface"
	"TikBase/pack/utils"
	"encoding/binary"
	"math"
	"sync/atomic"
	"time"
)

const (
	NeverExpire = 0
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMar    = math.MaxUint64 / 2
	scoreKeyPrefix    = "!score"
)

type Value struct {
	Data    []byte     // 数据
	TTL     int64      // 存活时间
	Created int64      // 数据创建时间
	Type    iface.Type // 数据类型
	Version uint32     // 版本号
}

type Meta struct {
	Expire   int64
	Version  int64
	Size     uint32
	Head     uint64
	Tail     uint64
	DataType iface.Type
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

func (v *Value) Score() float32 {
	return 0
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

func FindMeta(eng iface.Engine, key []byte, dataType iface.Type) (*Meta, error) {
	keyBytes := [][]byte{key}
	res := eng.Exec(iface.GET_STR, keyBytes)
	if res.Error() != nil {
		return &Meta{}, res.Error()
	}
	return DecodeMeta(res.Data()[0]), nil
}

func DecodeMeta(b []byte) *Meta {
	dataType := iface.Type(b[0])

	var index = 1
	expire, n := binary.Varint(b[index:])
	index += n
	version, n := binary.Varint(b[index:])
	index += n
	size, n := binary.Varint(b[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0

	if dataType == iface.LIST {
		head, n = binary.Uvarint(b[index:])
		index += n
		tail, _ = binary.Uvarint(b[index:])
	}

	return &Meta{
		Expire:  expire,
		Version: version,
		Size:    uint32(size),
		Head:    head,
		Tail:    tail,
	}
}

// Encode 编码元数据
func (meta *Meta) Encode() []byte {
	var size = maxMetadataSize

	if meta.DataType == iface.LIST {
		size += extraListMetaSize
	}
	b := make([]byte, size)

	var index = 1
	index += binary.PutVarint(b[index:], meta.Expire)
	index += binary.PutVarint(b[index:], meta.Version)
	index += binary.PutVarint(b[index:], int64(meta.Size))

	if meta.DataType == iface.LIST {
		index += binary.PutUvarint(b[index:], meta.Head)
		index += binary.PutUvarint(b[index:], meta.Tail)
	}

	return b[:index]
}

func EncodeMeta(meta *Meta) []byte {
	var size = maxMetadataSize

	if meta.DataType == iface.LIST {
		size += extraListMetaSize
	}
	b := make([]byte, size)

	var index = 1
	index += binary.PutVarint(b[index:], meta.Expire)
	index += binary.PutVarint(b[index:], meta.Version)
	index += binary.PutVarint(b[index:], int64(meta.Size))

	if meta.DataType == iface.LIST {
		index += binary.PutUvarint(b[index:], meta.Head)
		index += binary.PutUvarint(b[index:], meta.Tail)
	}

	return b[:index]
}
