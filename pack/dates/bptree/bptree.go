package bptree

import (
	"github.com/T4t4KAU/TikBase/engine/data"
	"github.com/T4t4KAU/TikBase/iface"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const indexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

type BPTree struct {
	tree *bbolt.DB
}

func New(dirPath string, sync bool) *BPTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !sync
	tree, err := bbolt.Open(filepath.Join(dirPath, indexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree at startup")
	}

	if err := tree.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(indexBucketName)
		return e
	}); err != nil {
		panic("failed to create bptree bucket at startup")
	}

	return &BPTree{tree: tree}
}

func (tree *BPTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := tree.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put index in bptree")
	}

	return true
}

func (tree *BPTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := tree.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get index in bptree")
	}

	return pos
}

func (tree *BPTree) Delete(key []byte) bool {
	if err := tree.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Delete(key)
	}); err != nil {
		panic("failed to delete index in bptree")
	}
	return true
}

func (tree *BPTree) Size() int {
	var size int
	if err := tree.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to size in bptree")
	}
	return size
}

func (tree *BPTree) Iterator(reverse bool) iface.Iterator {
	return newIterator(tree.tree, reverse)
}

type Iterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func (it *Iterator) Rewind() {
	if it.reverse {
		it.currKey, it.currValue = it.cursor.Last()
	} else {
		it.currKey, it.currValue = it.cursor.First()
	}
}

func (it *Iterator) Seek(key []byte) {
	it.currKey, it.currValue = it.cursor.Seek(key)
}

func (it *Iterator) Next() {
	if it.reverse {
		it.currKey, it.currValue = it.cursor.Prev()
	} else {
		it.currKey, it.currValue = it.cursor.Next()
	}
}

func (it *Iterator) Valid() bool {
	return len(it.currKey) != 0
}

func (it *Iterator) Key() []byte {
	return it.currKey
}

func (it *Iterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(it.currValue)
}

func (it *Iterator) Close() {
	_ = it.tx.Commit()
}

func newIterator(tree *bbolt.DB, reverse bool) *Iterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}

	it := &Iterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	it.Rewind()
	return it
}
