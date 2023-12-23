package bases

import (
	"bytes"
	"github.com/T4t4KAU/TikBase/iface"
)

type Iterator struct {
	iterator iface.Iterator
	base     *Base
	options  IteratorOptions
}

// NewIterator 创建迭代器
func (b *Base) NewIterator(opts IteratorOptions) *Iterator {
	it := b.index.Iterator(opts.Reverse)
	return &Iterator{
		base:     b,
		iterator: it,
		options:  opts,
	}
}

func (it *Iterator) Seek(key []byte) {
	it.iterator.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.iterator.Next()
	it.skipToNext()
}

func (it *Iterator) Value() ([]byte, error) {
	pos := it.iterator.Value()
	it.base.mutex.RLock()
	defer it.base.mutex.RUnlock()

	return it.base.getValueByPosition(pos)
}

func (it *Iterator) Valid() bool {
	return it.iterator.Valid()
}

func (it *Iterator) Rewind() {
	it.iterator.Rewind()
	it.skipToNext()
}

func (it *Iterator) Close() {
	it.iterator.Close()
}

func (it *Iterator) Key() []byte {
	return it.iterator.Key()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.iterator.Valid(); it.iterator.Next() {
		key := it.iterator.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
