package bases

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/errno"
	"TikBase/pack/utils"
	"encoding/binary"
	"time"
)

// Meta 元数据 支撑复杂数据类型
type Meta struct {
	Expire   int64
	Version  int64
	Size     uint32
	Head     uint64
	Tail     uint64
	DataType iface.Type
}

func newMeta(dataType iface.Type, expire int64, version int64, size uint32) *Meta {
	return &Meta{
		Expire:   expire,
		Version:  version,
		Size:     size,
		DataType: dataType,
	}
}

func (b *Base) FindMeta(key string, dataType iface.Type) (*Meta, error) {
	val, ok := b.Get(key)

	var meta *Meta
	var exist = true
	if !ok {
		exist = false
	} else {
		meta = DecodeMeta(val.Bytes())
		if meta.DataType != dataType {
			return nil, errno.ErrWrongTypeOperation
		}
		if meta.Expire != 0 && meta.Expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = newMeta(dataType, 0, time.Now().UnixNano(), 0)
		if dataType == iface.LIST {
			meta.Head = initialListFlag
			meta.Tail = initialListFlag
		}
	}

	return meta, nil
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

func (b *Base) HSet(key string, field, value []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.HASH)
	if err != nil {
		return false, err
	}

	encKey := NewHashInternalKey(key, meta.Version, field).Encode()
	_, ok := b.Get(utils.B2S(encKey))

	// 使用事务
	wb := b.NewWriteBatch()
	if !ok {
		// 不存在则更新数据
		meta.Size++
		_ = wb.Put(encKey, meta.Encode())
	}

	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return ok, nil
}

func (b *Base) HGet(key string, field []byte) (iface.Value, bool) {
	meta, err := b.FindMeta(key, iface.HASH)
	if err != nil {
		return nil, false
	}
	if meta.Size == 0 {
		return nil, false
	}

	strKey := NewHashInternalKey(key, meta.Version, field).String()
	return b.Get(strKey)
}

func (b *Base) HDel(key string, field []byte) bool {
	meta, err := b.FindMeta(key, iface.HASH)
	if err != nil {
		return false
	}
	if meta.Size == 0 {
		return false
	}

	encKey := NewHashInternalKey(key, meta.Version, field).Encode()
	_, ok := b.Get(utils.B2S(encKey))
	if ok {
		// 不存在则更新数据
		wb := b.NewWriteBatch()
		meta.Size--
		_ = wb.Put(encKey, meta.Encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false
		}
	}

	return ok
}

func (b *Base) SAdd(key string, member []byte) bool {
	meta, err := b.FindMeta(key, iface.SET)
	if err != nil {
		return false
	}

	encKey := NewSetInternalKey(key, meta.Version, member).Encode()
	_, ok := b.Get(utils.B2S(encKey))
	if !ok {
		wb := b.NewWriteBatch()
		meta.Size++
		_ = wb.Put(utils.S2B(key), encKey)
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false
		}
	}

	return ok
}

func (b *Base) Contain(key string, member []byte) bool {
	meta, err := b.FindMeta(key, iface.SET)
	if err != nil {
		return false
	}
	if meta.Size == 0 {
		return false
	}

	encKey := NewSetInternalKey(key, meta.Version, member).Encode()
	_, ok := b.Get(utils.B2S(encKey))
	return ok
}

func (b *Base) pushInner(key string, element []byte, isLeft bool) (uint32, error) {
	meta, err := b.FindMeta(key, iface.LIST)
	if err != nil {
		return 0, err
	}

	lk := NewListInternalKey(key, meta.Version, 0)
	if isLeft {
		lk.index = meta.Head - 1
	} else {
		lk.index = meta.Tail
	}

	wb := b.NewWriteBatch()
	meta.Size++
	if isLeft {
		meta.Head--
	} else {
		meta.Tail++
	}

	_ = wb.Put(utils.S2B(key), meta.Encode())
	_ = wb.Put(lk.Encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.Size, nil
}

func (b *Base) popInner(key string, isLeft bool) (iface.Value, error) {
	meta, err := b.FindMeta(key, iface.LIST)
	if err != nil {
		return nil, err
	}
	if meta.Size == 0 {
		return nil, nil
	}

	listKey := NewListInternalKey(key, meta.Version, 0)
	if isLeft {
		listKey.index = meta.Head
	} else {
		listKey.index = meta.Tail - 1
	}

	elem, ok := b.Get(listKey.String())
	if !ok {
		return nil, errno.ErrKeyNotFound
	}

	meta.Size--
	if isLeft {
		meta.Head++
	} else {
		meta.Tail--
	}

	v := values.NewMeta(meta.Encode())
	if !b.Set(key, &v) {
		return nil, err
	}
	return elem, nil
}

func (b *Base) LPush(key string, element []byte) (uint32, error) {
	return b.pushInner(key, element, true)
}

func (b *Base) RPush(key string, element []byte) (uint32, error) {
	return b.pushInner(key, element, false)
}

func (b *Base) LPop(key string) (iface.Value, error) {
	return b.popInner(key, true)
}

func (b *Base) RPop(key string) (iface.Value, error) {
	return b.popInner(key, false)
}
