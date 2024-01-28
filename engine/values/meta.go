package values

import (
	"encoding/binary"
	"github.com/T4t4KAU/TikBase/iface"
)

// Meta 元数据 支撑复杂数据类型
// 对于HASH、LIST、SET、ZSET 数据类型 存储引擎会先保存 对应的元信息
type Meta struct {
	Expire   int64
	Version  int64
	Size     uint32
	Head     uint64
	Tail     uint64
	DataType iface.Type
}

func NewMeta(dataType iface.Type, expire int64, version int64, size uint32) *Meta {
	return &Meta{
		Expire:   expire,
		Version:  version,
		Size:     size,
		DataType: dataType,
	}
}

func (meta *Meta) Value() Value {
	return New(meta.Encode(), 0, iface.META_DATA)
}

// DecodeMeta 解码元数据
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
		Expire:   expire,
		Version:  version,
		Size:     uint32(size),
		Head:     head,
		Tail:     tail,
		DataType: dataType,
	}
}

// Encode 将元数据编码
func (meta *Meta) Encode() []byte {
	var size = MaxMetadataSize

	if meta.DataType == iface.LIST {
		size += ExtraListMetaSize
	}
	b := make([]byte, size)
	b[0] = byte(meta.DataType)

	var index = 1
	index += binary.PutVarint(b[index:], meta.Expire)      // 过期时间
	index += binary.PutVarint(b[index:], meta.Version)     // 版本号
	index += binary.PutVarint(b[index:], int64(meta.Size)) // 数据大小

	// 对于列表类型 额外加入首位
	if meta.DataType == iface.LIST {
		index += binary.PutUvarint(b[index:], meta.Head)
		index += binary.PutUvarint(b[index:], meta.Tail)
	}

	return b[:index]
}

// EncodeMeta 元数据编码
func EncodeMeta(meta *Meta) []byte {
	var size = MaxMetadataSize

	if meta.DataType == iface.LIST {
		size += ExtraListMetaSize
	}
	b := make([]byte, size)
	b[0] = byte(meta.DataType)

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
