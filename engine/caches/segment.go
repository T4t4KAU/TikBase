package caches

import (
	"errors"
	"sync"
)

// 数据块
type segment struct {
	Data    map[string]*value
	Status  *Status
	options *Options
	mutex   *sync.RWMutex
}

// 返回一个使用options初始化过的segment实例
func newSegment(options *Options) *segment {
	return &segment{
		Data:    make(map[string]*value, options.MapSizeOfSegment),
		Status:  NewStatus(),
		options: options,
		mutex:   &sync.RWMutex{},
	}
}

// 返回指定key数据
func (seg *segment) get(key string) ([]byte, bool) {
	// 对当前segment加读锁
	seg.mutex.RLock()
	defer seg.mutex.RUnlock()
	// 获取从表中数据
	v, ok := seg.Data[key]
	if !ok {
		return nil, false
	}

	// 数据过期
	if !v.alive() {
		// 加写锁
		seg.mutex.Lock()
		seg.delete(key)
		seg.mutex.Unlock()
		return nil, false
	}
	return v.data(), true
}

// 将一个数据添加进segment
func (seg *segment) set(key string, value []byte, ttl int64) error {
	// 对当前segment进行加锁
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	// 检查是否以及存在
	if oldValue, ok := seg.Data[key]; ok {
		seg.Status.subEntry(key, oldValue.Data)
	}
	// 检查数据是否超出容量
	if !seg.checkEntryCapacity(key, value) {
		if oldValue, ok := seg.Data[key]; ok {
			seg.Status.addEntry(key, oldValue.Data)
		}
		return errors.New("the entry size will exceed if you set this entry")
	}

	// 修改状态消息
	seg.Status.addEntry(key, value)
	seg.Data[key] = newValue(value, ttl)
	return nil
}

// 从segment中删除指定key
func (seg *segment) delete(key string) {
	// 对当前segment加锁
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	if oldValue, ok := seg.Data[key]; ok {
		seg.Status.subEntry(key, oldValue.Data)
		delete(seg.Data, key)
	}
}

// 返回该segment状态
func (seg *segment) status() Status {
	seg.mutex.RLock()
	defer seg.mutex.RUnlock()
	return *seg.Status
}

// 判断segment数据容量是否已经到了设定的上限
func (seg *segment) checkEntryCapacity(newKey string, newValue []byte) bool {
	return seg.Status.entrySize()+int64(len(newKey))+int64(len(newValue)) <=
		int64((seg.options.MaxEntrySize*1024*1024)/seg.options.SegmentSize)
}

// 清理segment中过期数据
func (seg *segment) gc() {
	// 对GC的segment加锁
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	count := 0

	// 遍历segment中数据
	for k, v := range seg.Data {
		if !v.alive() {
			seg.Status.subEntry(k, v.Data)
			delete(seg.Data, k)
			count++
			if count >= seg.options.MaxGcCount {
				break
			}
		}
	}
}
