package bases

import (
	"errors"
	"github.com/T4t4KAU/TikBase/engine/values"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"time"
)

// FindMeta 查找元信息
func (b *Base) FindMeta(key string, dataType iface.Type) (*values.Meta, error) {
	val, err := b.Get(key)
	if err != nil && !errors.Is(err, errno.ErrKeyNotFound) {
		return nil, err
	}

	var meta *values.Meta
	var exist = true
	if errors.Is(err, errno.ErrKeyNotFound) {
		exist = false
	} else {
		meta = values.DecodeMeta(val.Bytes()) // 解析元信息
		if meta.DataType != dataType {
			return nil, errno.ErrWrongTypeOperation
		}
		if meta.Expire != 0 && meta.Expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		// 不存在则创建
		meta = values.NewMeta(dataType, 0, time.Now().UnixNano(), 0)
		// 对于LIST 要初始化首尾
		if dataType == iface.LIST {
			meta.Head = values.InitialListFlag
			meta.Tail = values.InitialListFlag
		}
	}

	return meta, nil
}

// HSet HashSet操作
func (b *Base) HSet(key string, field, value []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.HASH) // 查找元信息
	if err != nil {
		return false, err
	}

	encKey := values.NewHashInternalKey(key, meta.Version, field).Encode()

	var exist = true
	if _, err = b.Get(utils.B2S(encKey)); errors.Is(err, errno.ErrKeyNotFound) {
		exist = false
	}

	// 创建结构元信息和添加元素放在一个事务中操作
	wb := b.NewWriteBatch()
	if !exist {
		// 不存在则追加
		meta.Size++
		_ = wb.Put(utils.S2B(key), meta.Encode())
	}

	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

// HGet HashGet操作
func (b *Base) HGet(key string, field []byte) (iface.Value, error) {
	meta, err := b.FindMeta(key, iface.HASH)
	if err != nil {
		return nil, err
	}
	if meta.Size == 0 {
		return nil, nil
	}

	hashKey := values.NewHashInternalKey(key, meta.Version, field).String()
	return b.Get(hashKey)
}

// HDel HashDelete操作
func (b *Base) HDel(key string, field []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.HASH)
	if err != nil {
		return false, err
	}
	if meta.Size == 0 {
		return false, errno.ErrHashDataIsEmpty
	}

	encKey := values.NewHashInternalKey(key, meta.Version, field).Encode()

	var exist = true
	if _, err = b.Get(utils.B2S(encKey)); errors.Is(err, errno.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		// 不存在则更新数据
		wb := b.NewWriteBatch()
		meta.Size--
		_ = wb.Put(encKey, meta.Encode()) // 修改元信息
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// SAdd 向集合添加元素
func (b *Base) SAdd(key string, member []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.SET)
	if err != nil {
		return false, err
	}

	encKey := values.NewSetInternalKey(key, meta.Version, member).Encode()

	if _, err = b.Get(utils.B2S(encKey)); errors.Is(err, errno.ErrKeyNotFound) {
		wb := b.NewWriteBatch()
		meta.Size++
		_ = wb.Put(utils.S2B(key), meta.Encode())
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return true, nil
}

// SIsMember 判断元素是否在集合中
func (b *Base) SIsMember(key string, member []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.SET)
	if err != nil {
		return false, err
	}
	if meta.Size == 0 {
		return false, errno.ErrSetDataIsEmpty
	}

	encKey := values.NewSetInternalKey(key, meta.Version, member).Encode()
	_, err = b.Get(utils.B2S(encKey))
	if err != nil && !errors.Is(err, errno.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, errno.ErrKeyNotFound) {
		return false, errno.ErrSetMemberNotFound
	}

	return true, nil
}

func (b *Base) SRem(key string, member []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.SET)
	if err != nil {
		return false, err
	}
	if meta.Size == 0 {
		return false, errno.ErrSetDataIsEmpty
	}

	setKey := values.NewSetInternalKey(key, meta.Version, member).Encode()
	if _, err = b.Get(utils.B2S(setKey)); errors.Is(err, errno.ErrKeyNotFound) {
		return false, errno.ErrSetMemberNotFound
	}

	wb := b.NewWriteBatch()
	meta.Size--
	_ = wb.Put(utils.S2B(key), meta.Encode())
	_ = wb.Delete(setKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Base) pushInner(key string, member []byte, isLeft bool) (uint32, error) {
	meta, err := b.FindMeta(key, iface.LIST)
	if err != nil {
		return 0, err
	}

	listKey := values.NewListInternalKey(key, meta.Version, 0)
	if isLeft {
		listKey.Index = meta.Head - 1
	} else {
		listKey.Index = meta.Tail
	}

	wb := b.NewWriteBatch()
	meta.Size++
	if isLeft {
		meta.Head--
	} else {
		meta.Tail++
	}

	_ = wb.Put(utils.S2B(key), meta.Encode())
	_ = wb.Put(listKey.Encode(), member)

	// 事务提交
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.Size, nil
}

func (b *Base) popInner(key string, isLeft bool) (iface.Value, error) {
	meta, err := b.FindMeta(key, iface.LIST)
	if err != nil {
		return nil, err
	}
	if meta.Size == 0 {
		return nil, nil
	}

	listKey := values.NewListInternalKey(key, meta.Version, 0)
	if isLeft {
		listKey.Index = meta.Head
	} else {
		listKey.Index = meta.Tail - 1
	}

	elem, err := b.Get(listKey.String())
	if err != nil {
		return nil, err
	}

	meta.Size--
	if isLeft {
		meta.Head++
	} else {
		meta.Tail--
	}

	val := meta.Value()
	if err = b.Set(key, &val); err != nil {
		return nil, err
	}
	return elem, nil
}

func (b *Base) LPush(key string, member []byte) (uint32, error) {
	return b.pushInner(key, member, true)
}

func (b *Base) RPush(key string, member []byte) (uint32, error) {
	return b.pushInner(key, member, false)
}

func (b *Base) LPop(key string) (iface.Value, error) {
	return b.popInner(key, true)
}

func (b *Base) RPop(key string) (iface.Value, error) {
	return b.popInner(key, false)
}

func (b *Base) ZAdd(key string, score float64, member []byte) (bool, error) {
	meta, err := b.FindMeta(key, iface.ZSET)
	if err != nil {
		return false, err
	}

	zsetKey := values.NewZSetInternalKey(key, meta.Version, member, score)

	var exist = true
	// 先查看是否存在
	val, err := b.Get(utils.B2S(zsetKey.EncodeWithMember()))
	if err != nil && !errors.Is(err, errno.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, errno.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		// 权值相等 直接返回
		if score == val.Score() {
			return false, nil
		}
	}

	wb := b.NewWriteBatch()
	if !exist {
		meta.Size++
		_ = wb.Put(utils.S2B(key), meta.Encode())
	}

	if exist {
		oldKey := values.NewZSetInternalKey(key, meta.Version, member, val.Score())
		_ = wb.Delete(oldKey.EncodeWithScore())
	}

	_ = wb.Put(zsetKey.EncodeWithMember(), utils.F642B(score))
	_ = wb.Put(zsetKey.EncodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (b *Base) ZScore(key string, member []byte) (float64, error) {
	meta, err := b.FindMeta(key, iface.ZSET)
	if err != nil {
		return -1, err
	}
	if meta.Size == 0 {
		return -1, errno.ErrZSetDataIsEmpty
	}

	zsetKey := values.NewZSetInternalKey(key, meta.Version, member, 0)
	val, err := b.Get(utils.B2S(zsetKey.EncodeWithMember()))
	if err != nil {
		return -1, err
	}

	return val.Score(), nil
}
