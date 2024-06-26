package bases

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/engine/values"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strconv"
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
	dir, _ := os.MkdirTemp("", "github.com/T4t4KAU/TikBase")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	// defer destroyDB(b)

	assert.Nil(t, err)
	assert.NotNil(t, b)
}

func TestBase_Set(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "test")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	assert.Nil(t, err)
	assert.NotNil(t, b)

	v := values.New([]byte(utils.GenerateRandomString(10)), 0, iface.STRING)
	err = b.Set("test", &v)
	assert.Nil(t, err)

	for i := 0; i < 100000; i++ {
		v = values.New([]byte(utils.GenerateRandomString(10)), 0, iface.STRING)
		err = b.Set(utils.GenerateRandomString(10), &v)
		assert.Nil(t, err)
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
	dir, err := os.MkdirTemp("", "github.com/T4t4KAU/TikBase")
	assert.Nil(t, err)

	opts.DirPath = dir
	base, err := NewBaseWith(opts)

	defer destroyDB(base)

	assert.Nil(t, err)
	assert.NotNil(t, base)

	v1 := values.New([]byte("value1"), 0, iface.STRING)
	err = base.Set("key1", &v1)
	assert.Nil(t, err)

	v2 := values.New([]byte("value2"), 0, iface.STRING)
	err = base.Set("key2", &v2)
	assert.Nil(t, err)

	v3 := values.New([]byte("value3"), 0, iface.STRING)
	err = base.Set("key3", &v3)
	assert.Nil(t, err)

	it1 := base.NewIterator(DefaultIteratorOptions)
	for it1.Rewind(); it1.Valid(); it1.Next() {
		assert.NotNil(t, it1.Key())
		v, _ := it1.Value()
		t.Logf("%v %v", string(it1.Key()), string(v))
	}
}

func TestBase_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "github.com/T4t4KAU/TikBase")
	opts.DirPath = dir
	base, err := NewBaseWith(opts)

	defer destroyDB(base)

	assert.Nil(t, err)
	assert.NotNil(t, base)

	wb := base.NewWriteBatchWith(DefaultWriteBatchOptions)
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	_, err = base.Get("key1")
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	v, err := base.Get("key1")
	assert.Nil(t, err)

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
	err = b1.Set("key1", &v1)
	assert.Nil(t, err)

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

	_, err = b2.Get("key1")
	assert.Nil(t, err)
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

func TestBase_HGet(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "../../temp"
	base, err := NewBaseWith(opts)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = base.HSet("test_hash", []byte("test_key"), []byte("test_value"))
	if err != nil {
		t.Error(err)
		return
	}

	res, err := base.HGet("test_hash", []byte("test_key"))
	if err != nil {
		t.Log("HGet failed")
		return
	}

	assert.Equal(t, "test_value", res.String())
}

func TestBase_HDel(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "../../temp"
	base, err := NewBaseWith(opts)
	assert.Nil(t, err)

	_, err = base.HSet("test_hash", []byte("test_key"), []byte("test_value"))
	assert.Nil(t, err)

	res, err := base.HDel("test_hash", []byte("test_key"))
	assert.Nil(t, err)
	assert.True(t, res)

	_, err = base.HGet("test_hash", []byte("test_key"))
	assert.Equal(t, errno.ErrHashKeyNotFound, err)
}

func TestBase_SAdd(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "../../temp"
	base, err := NewBaseWith(opts)
	assert.Nil(t, err)

	_, err = base.SAdd("test_set", []byte("test_element1"))
	assert.Nil(t, err)

	ok, err := base.SIsMember("test_set", []byte("test_element1"))
	assert.Nil(t, err)
	assert.True(t, ok)
}

func TestBase_SRem(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "../../temp"
	base, err := NewBaseWith(opts)
	assert.Nil(t, err)

	_, err = base.SAdd("test_set", []byte("test_element1"))
	assert.Nil(t, err)

	ok, err := base.SIsMember("test_set", []byte("test_element1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = base.SRem("test_set", []byte("test_element1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = base.SIsMember("test_set", []byte("test_element1"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestBase_LPush(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "../../temp"
	base, err := NewBaseWith(opts)
	if err != nil {
		t.Error(err)
		return
	}

	n, err := base.LPush("test_list", []byte("001"))
	assert.Nil(t, err)
	assert.Equal(t, n, uint32(1))

	n, err = base.LPush("test_list", []byte("002"))
	assert.Nil(t, err)
	assert.Equal(t, n, uint32(2))

	v, err := base.LPop("test_list")
	assert.Nil(t, err)

	t.Log(v.String())
}

func BenchmarkBase_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "test")
	opts.DirPath = dir
	base, err := NewBaseWith(opts)
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		v := values.New([]byte("value"), 0, iface.STRING)
		_ = base.Set(fmt.Sprintf("key%d", i), &v)
		assert.Nil(b, err)
	}
}

func BenchmarkBase_Get(b *testing.B) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "test")
	opts.DirPath = dir
	base, err := NewBaseWith(opts)
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		v := values.New([]byte("value"), 0, iface.STRING)
		_ = base.Set(fmt.Sprintf("key%d", i), &v)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err = base.Get(fmt.Sprintf("key%d", i))
		assert.Nil(b, err)
	}
}

func BenchmarkBase_Delete(b *testing.B) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "test")
	opts.DirPath = dir
	base, err := NewBaseWith(opts)
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		v := values.New([]byte("value"), 0, iface.STRING)
		err = base.Set(fmt.Sprintf("key%d", i), &v)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = base.Del(fmt.Sprintf("key%d", i))
		assert.Nil(b, err)
	}
}

func BenchmarkBolt_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	db, err := bolt.Open("test.db", os.ModePerm, nil)
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
			if err != nil {
				return err
			}
			key := []byte("key" + strconv.Itoa(i))
			value := []byte("value" + strconv.Itoa(i))
			err = bucket.Put(key, value)
			return err
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}
