package artree

import (
	"TikBase/engine/data"
	"TikBase/iface"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type ARTree struct {
	tree  goart.Tree
	mutex sync.RWMutex
}

func New() *ARTree {
	return &ARTree{
		tree: goart.New(),
	}
}

func (tree *ARTree) Get(key []byte) *data.LogRecordPos {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()

	val, ok := tree.tree.Search(key)
	if !ok {
		return nil
	}

	return val.(*data.LogRecordPos)
}

func (tree *ARTree) Delete(key []byte) bool {
	tree.mutex.Lock()
	_, deleted := tree.tree.Delete(key)
	tree.mutex.Unlock()
	return deleted
}

func (tree *ARTree) Size() int {
	tree.mutex.RLock()
	size := tree.tree.Size()
	tree.mutex.RUnlock()

	return size
}

func (tree *ARTree) Iterator(reverse bool) iface.Iterator {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()

	return tree.newIterator(tree.tree, reverse)
}

func (tree *ARTree) Put(key []byte, pos *data.LogRecordPos) bool {
	tree.mutex.Lock()
	tree.tree.Insert(key, pos)
	tree.mutex.Unlock()

	return true
}

type Iterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

func (it *Iterator) Rewind() {
	it.currIndex = 0
}

func (it *Iterator) Seek(key []byte) {
	if it.reverse {
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) <= 0
		})
	} else {
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) >= 0
		})
	}
}

func (it *Iterator) Next() {
	it.currIndex += 1
}

func (it *Iterator) Valid() bool {
	return it.currIndex < len(it.values)
}

func (it *Iterator) Key() []byte {
	return it.values[it.currIndex].key
}

func (it *Iterator) Value() *data.LogRecordPos {
	return it.values[it.currIndex].pos
}

func (it *Iterator) Close() {
	it.values = nil
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (tree *ARTree) newIterator(t goart.Tree, reverse bool) *Iterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}

		return true
	}

	t.ForEach(saveValues)

	return &Iterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}
