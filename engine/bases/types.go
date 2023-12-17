package bases

import (
	"TikBase/iface"
	"TikBase/pack/errno"
	"TikBase/pack/utils"
	"encoding/binary"
	"math"
	"time"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListFlag   = math.MaxUint64 / 2
	scoreKeyPrefix    = "!score"
)

type HashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func NewHashInternalKey(key string, version int64, field []byte) *HashInternalKey {
	return &HashInternalKey{
		key:     utils.S2B(key),
		version: version,
		field:   field,
	}
}

func (hk *HashInternalKey) Encode() []byte {
	b := make([]byte, len(hk.key)+len(hk.field)+8)

	var index = 0
	copy(b[index:index+len(hk.key)], hk.key)
	binary.LittleEndian.PutUint64(b[index:index+8], uint64(hk.version))
	index += 8
	copy(b[index:], hk.field)

	return b
}

func (hk *HashInternalKey) String() string {
	return utils.B2S(hk.Encode())
}

type SetInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func NewSetInternalKey(key string, version int64, member []byte) *SetInternalKey {
	return &SetInternalKey{
		key:     utils.S2B(key),
		version: version,
		member:  member,
	}
}

func (sk *SetInternalKey) Encode() []byte {
	b := make([]byte, len(sk.key)+len(sk.member)+12)

	var index = 0
	copy(b[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	binary.LittleEndian.PutUint64(b[index:index+8], uint64(sk.version))
	index += 8

	copy(b[index:index+len(sk.member)], sk.member)
	index += len(sk.member)
	binary.LittleEndian.PutUint32(b[index:], uint32(len(sk.member)))

	return b
}

type ZSetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func NewZSetInternalKey(key []byte, version int64, member []byte, score float64) *ZSetInternalKey {
	return &ZSetInternalKey{
		key:     key,
		version: version,
		member:  member,
		score:   score,
	}
}

type ListInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

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
