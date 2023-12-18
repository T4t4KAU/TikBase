package bases

import (
	"TikBase/pack/utils"
	"encoding/binary"
	"math"
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

func (sk *SetInternalKey) String() string {
	return utils.B2S(sk.Encode())
}

type ListInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func NewListInternalKey(key string, version int64, index uint64) *ListInternalKey {
	return &ListInternalKey{
		key:     utils.S2B(key),
		version: version,
		index:   index,
	}
}

func (lk *ListInternalKey) Encode() []byte {
	b := make([]byte, len(lk.key)+8+8)
	var index = 0

	copy(b[index:index+len(lk.key)], lk.key)
	index += len(lk.key)
	binary.LittleEndian.PutUint64(b[index:index+8], uint64(lk.version))

	return b
}

func (lk *ListInternalKey) String() string {
	return utils.B2S(lk.Encode())
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

func (zk *ZSetInternalKey) EncodeWithMember() []byte {
	b := make([]byte, len(zk.key)+len(zk.member)+8)

	var index = 0
	copy(b[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	binary.LittleEndian.PutUint64(b[index:index+8], uint64(zk.version))
	index += 8
	copy(b[index:], zk.member)

	return b
}

func (zk *ZSetInternalKey) EncodeWithScore() []byte {
	scoreBuf := utils.F642B(zk.score)
	b := make([]byte, len(zk.key)+len(scoreKeyPrefix)+len(scoreBuf)+len(zk.member)+8+4)

	var index = 0
	copy(b[index:index+len(scoreKeyPrefix)], scoreKeyPrefix)
	index += len(scoreKeyPrefix)

	copy(b[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	binary.LittleEndian.PutUint64(b[index:index+8], uint64(zk.version))
	index += 8

	copy(b[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	binary.LittleEndian.PutUint32(b[index:], uint32(len(zk.member)))
	return b
}
