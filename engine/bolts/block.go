package bolts

import (
	"bytes"
	"encoding/binary"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"io"
)

type Block struct {
	buffer       [30]byte
	record       *bytes.Buffer
	entriesCount int
	prevKey      []byte
}

type MemTableConstructor func() iface.MemTable

type Index struct {
	Key             []byte
	PrevBlockOffset uint64
	PrevBlockSize   uint64
}

func (b *Block) Append(key, value []byte) {
	defer func() {
		b.prevKey = append(b.prevKey[:0], key...)
		b.entriesCount++
	}()

	// 公共前缀长度
	sharedPrefixLen := utils.SharePrefixLen(b.prevKey, key)

	n := binary.PutUvarint(b.buffer[0:], uint64(sharedPrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(key)-sharedPrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(value)))

	_, _ = b.record.Write(b.buffer[:n])
	b.record.Write(key[sharedPrefixLen:])
	b.record.Write(value)
}

func (b *Block) Size() int {
	return b.record.Len()
}

func (b *Block) FlushTo(dest io.Writer) (uint64, error) {
	defer b.clear()
	n, err := dest.Write(b.ToBytes())
	return uint64(n), err
}

func (b *Block) ToBytes() []byte {
	return b.record.Bytes()
}

func (b *Block) clear() {
	b.entriesCount = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
}
