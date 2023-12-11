package bases

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDB(base *Base) {
	if base != nil {
		if base.activeFile != nil {
			_ = base.Close()
		}
		err := os.RemoveAll(base.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestNew(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "tikbase")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	// defer destroyDB(b)

	assert.Nil(t, err)
	assert.NotNil(t, b)
}

func TestBase_Set(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "tikbase")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	assert.Nil(t, err)
	assert.NotNil(t, b)

	v := values.New([]byte(utils.GenerateRandomString(10)), 0, iface.STRING)
	res := b.Set("test", &v)
	assert.True(t, res)

	for i := 0; i < 100000; i++ {
		v = values.New([]byte(utils.GenerateRandomString(10)), 0, iface.STRING)
		res = b.Set(utils.GenerateRandomString(10), &v)
		assert.True(t, res)
	}
}

func TestBase_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "temp")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	assert.Nil(t, err)
	assert.NotNil(t, b)

	iterator := b.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestBase_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, err := os.MkdirTemp("", "tikbase")
	assert.Nil(t, err)

	opts.DirPath = dir
	base, err := NewBaseWith(opts)

	defer destroyDB(base)

	assert.Nil(t, err)
	assert.NotNil(t, base)

	v1 := values.New([]byte("value1"), 0, iface.STRING)
	ok1 := base.Set("key1", &v1)
	assert.True(t, ok1)

	v2 := values.New([]byte("value2"), 0, iface.STRING)
	ok2 := base.Set("key2", &v2)
	assert.True(t, ok2)

	v3 := values.New([]byte("value3"), 0, iface.STRING)
	ok3 := base.Set("key3", &v3)
	assert.True(t, ok3)

	it1 := base.NewIterator(DefaultIteratorOptions)
	for it1.Rewind(); it1.Valid(); it1.Next() {
		assert.NotNil(t, it1.Key())
		v, _ := it1.Value()
		t.Logf("%v %v", string(it1.Key()), string(v))
	}
}

func TestBase_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "tikbase")
	opts.DirPath = dir
	base, err := NewBaseWith(opts)

	defer destroyDB(base)

	assert.Nil(t, err)
	assert.NotNil(t, base)

	wb := base.NewWriteBatchWith(DefaultWriteBatchOptions)
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	_, ok := base.Get("key1")
	assert.False(t, ok)

	err = wb.Commit()
	assert.Nil(t, err)

	v, ok := base.Get("key1")
	assert.True(t, ok)

	t.Log(v.String())
}

func TestBase_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "temp")
	opts.DirPath = dir
	b1, err := NewBaseWith(opts)
	defer destroyDB(b1)

	assert.Nil(t, err)
	assert.NotNil(t, b1)

	v1 := values.New([]byte("value1"), 0, iface.STRING)
	ok1 := b1.Set("key1", &v1)
	assert.True(t, ok1)

	wb := b1.NewWriteBatch()

	v2 := values.New([]byte("value2"), 0, iface.STRING)
	err = wb.Put([]byte("key2"), v2.Bytes())
	assert.Nil(t, err)

	err = wb.Delete([]byte("key1"))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = b1.Close()
	assert.Nil(t, err)

	b2, err := NewBaseWith(opts)
	assert.Nil(t, err)

	_, ok := b2.Get("key1")
	assert.False(t, ok)
}

func TestBase_Merge(t *testing.T) {
	opts := DefaultOptions
	dir := "/tmp/merge"
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	base, err := NewBaseWith(opts)
	assert.Nil(t, err)
	assert.NotNil(t, base)
}

func TestBase_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "test")
	opts.DirPath = dir
	base1, err := NewBaseWith(opts)
	defer destroyDB(base1)
	assert.Nil(t, err)
	assert.NotNil(t, base1)

	base2, err := NewBaseWith(opts)
	t.Log(base2)
	t.Log(err)
}
