package bases

import (
	"encoding/binary"
	"github.com/T4t4KAU/TikBase/pack/utils"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListFlag   = math.MaxUint64 / 2
	scoreKeyPrefix    = "!score"
)

// HashInternalKey 用于标识一个HASH结构
type HashInternalKey struct {
	key     []byte
	version int64 // 版本号
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
	b := make([]byte, len(hk.key)+8+len(hk.field))

	var index = 0

	// 将key复制到数组
	copy(b[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	binary.LittleEndian.PutUint64(b[index:index+8], uint64(hk.version))
	index += 8

	// 将field复制到数组
	copy(b[index:], hk.field)

	return b
}

func (hk *HashInternalKey) String() string {
	return utils.B2S(hk.Encode())
}

// SetInternalKey 用于标识一个SET结构
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

	// 将key复制到数组
	copy(b[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// 写入版本号
	binary.LittleEndian.PutUint64(b[index:index+8], uint64(sk.version))
	index += 8

	// 将member复制到数组
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
	version int64 // 版本号
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
	index += 8

	binary.LittleEndian.PutUint64(b[index:], lk.index)

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

func NewZSetInternalKey(key string, version int64, member []byte, score float64) *ZSetInternalKey {
	return &ZSetInternalKey{
		key:     utils.S2B(key),
		version: version,
		member:  member,
		score:   score,
	}
}

func (zk *ZSetInternalKey) EncodeWithMember() []byte {
	b := make([]byte, len(zk.key)+len(zk.member)+8)

	var index = 0

	// 写入key
	copy(b[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// 写入版本号
	binary.LittleEndian.PutUint64(b[index:index+8], uint64(zk.version))
	index += 8
	copy(b[index:], zk.member)

	return b
}

func (zk *ZSetInternalKey) EncodeWithScore() []byte {
	scoreBytes := utils.F642B(zk.score)
	b := make([]byte, len(zk.key)+len(scoreKeyPrefix)+len(scoreBytes)+len(zk.member)+8+4)

	var index = 0
	copy(b[index:index+len(scoreKeyPrefix)], scoreKeyPrefix)
	index += len(scoreKeyPrefix)

	copy(b[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	binary.LittleEndian.PutUint64(b[index:index+8], uint64(zk.version))
	index += 8

	copy(b[index:index+len(scoreBytes)], scoreBytes)
	index += len(scoreBytes)

	copy(b[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	binary.LittleEndian.PutUint32(b[index:], uint32(len(zk.member)))
	return b
}
