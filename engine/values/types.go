package values

import "encoding/binary"

type HashTypeKey struct {
	key     []byte
	version int64
	field   []byte
}

func NewHashInternalKey(key []byte, version int64, field []byte) *HashTypeKey {
	return &HashTypeKey{
		key:     key,
		version: version,
		field:   field,
	}
}

func (hk *HashTypeKey) Encode() []byte {
	b := make([]byte, len(hk.key)+len(hk.field)+8)

	var index = 0
	copy(b[index:index+len(hk.key)], hk.key)
	binary.LittleEndian.PutUint64(b[index:index+8], uint64(hk.version))
	index += 8
	copy(b[index:], hk.field)

	return b
}

type SetInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func NewSetInternalKey(key []byte, version int64, member []byte) *SetInternalKey {
	return &SetInternalKey{
		key:     key,
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
