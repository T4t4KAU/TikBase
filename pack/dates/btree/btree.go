package btree

import (
	"TikBase/engine/data"
	"bytes"
	"github.com/google/btree"
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

func (tree *Tree) Put(key []byte, pos *data.LogRecordPos) bool {
	tree.mutex.Lock()
	tree.tree.ReplaceOrInsert(&Item{key: key, pos: pos})
	tree.mutex.Unlock()

	return true
}

func (tree *Tree) Get(key []byte) *data.LogRecordPos {
	if itv := tree.tree.Get(&Item{key: key}); itv != nil {
		return itv.(*Item).pos
	}
	return nil
}

func (tree *Tree) Delete(key []byte) bool {
	oit := &Item{key: key}
	tree.mutex.Lock()
	oldItem := tree.tree.Delete(oit)
	tree.mutex.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
