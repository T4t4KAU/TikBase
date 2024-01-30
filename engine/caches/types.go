package caches

import (
	"errors"
	"github.com/T4t4KAU/TikBase/engine/values"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"time"
)

func (c *Cache) FindMeta(key string, dataType iface.Type) (*values.Meta, error) {
	val, err := c.Get(key)
	if err != nil && !errors.Is(err, errno.ErrKeyNotFound) {
		return nil, err
	}

	var meta *values.Meta
	var exist = true
	if errors.Is(err, errno.ErrKeyNotFound) {
		exist = false
	} else {
		meta = values.DecodeMeta(val.Bytes())
		if meta.DataType != dataType {
			return nil, errno.ErrWrongTypeOperation
		}
		if meta.Expire != 0 && meta.Expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = values.NewMeta(dataType, 0, time.Now().UnixNano(), 0)
		if dataType == iface.LIST {
			meta.Head = values.InitialListFlag
			meta.Tail = values.InitialListFlag
		}
	}

	return meta, nil
}

func (c *Cache) HSet(key string, field, value []byte) (bool, error) {
	meta, err := c.FindMeta(key, iface.HASH)
	if err != nil {
		return false, err
	}

	hashKey := values.NewHashInternalKey(key, meta.Version, field).String()

	var exist = true
	if _, err = c.Get(hashKey); errors.Is(err, errno.ErrKeyNotFound) {
		exist = false
	}
	if !exist {
		meta.Size++
	}

	return true, nil
}

func (c *Cache) HGet(key string, field []byte) (iface.Value, error) {
	meta, err := c.FindMeta(key, iface.HASH)
	if err != nil {
		return nil, err
	}
	if meta.Size == 0 {
		return nil, nil
	}
	hashKey := values.NewHashInternalKey(key, meta.Version, field).String()
	return c.Get(hashKey)
}
