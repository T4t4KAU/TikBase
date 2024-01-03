package bptree

import (
	"go.etcd.io/bbolt"
	"testing"
)

func TestBPTree_Put(t *testing.T) {
	bt, _ := bbolt.Open("/tmp/test", 0644, nil)
	bt.Update(func(tx *bbolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte("name"))
		bucket.Put([]byte("bbccde"), []byte("b1"))
		bucket.Put([]byte("cchune"), []byte("b1"))
		bucket.Put([]byte("bbcaed"), []byte("b1"))
		bucket.Put([]byte("aacded"), []byte("b1"))
		bucket.Put([]byte("ccdeas"), []byte("b1"))
		return nil
	})

	bt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("name"))
		cursor := bucket.Cursor()
		k, _ := cursor.Seek([]byte("bb"))
		t.Log(string(k))
		return nil
	})
}
