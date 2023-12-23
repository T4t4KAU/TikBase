package artree

import (
	"github.com/T4t4KAU/TikBase/engine/data"
	"testing"
)

func TestARTree_Put(t *testing.T) {
	art := New()
	art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})

	art.Put(nil, nil)
	t.Log(art.Size())
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := New()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 11, Offset: 123})

	val := art.Get([]byte("key1"))
	t.Log(val)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := New()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 11, Offset: 123})

	b := art.Delete([]byte("key1"))
	t.Log(b)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := New()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key4"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("key5"), &data.LogRecordPos{Fid: 11, Offset: 123})

	art.Iterator(true)
}
