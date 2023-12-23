package btree

import (
	"bytes"
	"github.com/T4t4KAU/TikBase/engine/data"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/google/btree"
	"sort"
	"sync"
)

// B Tree

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Tree struct {
	tree  *btree.BTree
	mutex *sync.RWMutex
}

func New() *Tree {
	return &Tree{
		tree:  btree.New(32),
		mutex: new(sync.RWMutex),
	}
}

func (tree *Tree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	tree.mutex.Lock()
	old := tree.tree.ReplaceOrInsert(&Item{key: key, pos: pos})
	tree.mutex.Unlock()
	if old == nil {
		return nil
	}

	return old.(*Item).pos
}

func (tree *Tree) Get(key []byte) *data.LogRecordPos {
	if itv := tree.tree.Get(&Item{key: key}); itv != nil {
		return itv.(*Item).pos
	}
	return nil
}

func (tree *Tree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	tree.mutex.Lock()
	oldItem := tree.tree.Delete(it)
	tree.mutex.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (tree *Tree) Size() int {
	return tree.tree.Len()
}

func (tree *Tree) Close() error {
	return nil
}

type iterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

func (it *iterator) Rewind() {
	it.currIndex = 0
}

func (it *iterator) Seek(key []byte) {
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

func (it *iterator) Next() {
	it.currIndex += 1
}

func (it *iterator) Valid() bool {
	return it.currIndex < len(it.values)
}

func (it *iterator) Key() []byte {
	return it.values[it.currIndex].key
}

func (it *iterator) Value() *data.LogRecordPos {
	return it.values[it.currIndex].pos
}

func (it *iterator) Close() {
	it.values = nil
}

func (tree *Tree) Iterator(reverse bool) iface.Iterator {
	if tree.tree == nil {
		return nil
	}
	tree.mutex.RLock()
	defer tree.mutex.RLock()

	return newBTreeIterator(tree.tree, reverse)
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *iterator {
	var idx int

	values := make([]*Item, tree.Len())

	// 将所有数据放到数组中
	f := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(f)
	} else {
		tree.Ascend(f)
	}

	return &iterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}
