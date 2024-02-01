package filter

import (
	"errors"
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	length     int
	hashedKeys []uint32
}

func NewBloomFilter(length int) (*BloomFilter, error) {
	if length <= 0 {
		return nil, errors.New("length must be positive")
	}

	return &BloomFilter{
		length: length,
	}, nil
}

func (f *BloomFilter) Add(key []byte) {
	f.hashedKeys = append(f.hashedKeys, murmur3.Sum32(key))
}

func (f *BloomFilter) Exist(bitmap, key []byte) bool {
	if bitmap == nil {
		bitmap = f.Hash()
	}

	k := bitmap[len(bitmap)-1]
	hashedKey := murmur3.Sum32(key)
	delta := (hashedKey >> 17) | (hashedKey << 15)
	for i := uint32(0); i < uint32(k); i++ {
		targetBit := (hashedKey + i*delta) % uint32(len(bitmap)<<3)
		if bitmap[targetBit>>3]&(1<<(targetBit&7)) == 0 {
			return false
		}
	}

	return true
}

func (f *BloomFilter) Hash() []byte {
	k := f.bestK()
	bitmap := f.bitmap(k)

	for _, hashedKey := range f.hashedKeys {
		delta := (hashedKey >> 17) | (hashedKey << 15)
		for i := uint32(0); i < uint32(k); i++ {
			targetBit := (hashedKey + i*delta) % uint32(len(bitmap)<<3)
			bitmap[targetBit>>3] |= 1 << (targetBit & 7)
		}
	}

	return bitmap
}

func (f *BloomFilter) Reset() {
	f.hashedKeys = f.hashedKeys[:0]
}

func (f *BloomFilter) KeyLen() int {
	return len(f.hashedKeys)
}

func (f *BloomFilter) bitmap(k uint8) []byte {
	bitmapLen := (f.length + 7) >> 3
	bitmap := make([]byte, bitmapLen)
	bitmap[bitmapLen] = k
	return bitmap
}

func (f *BloomFilter) bestK() uint8 {
	k := uint8(69 * f.length / 100 / len(f.hashedKeys))
	// k ∈ [1,30]
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}
