package caches

import (
	"TikBase/engine/values"
	"TikBase/pack/iface"

	"errors"
	"sync"
)

// 数据块
type segment struct {
	Data    map[string]*values.Value // 哈希表存放数据
	Status  *Status                  // 状态信息
	options *Options                 // 配置信息
	mutex   *sync.RWMutex            // 读写锁
}

// 返回一个使用options初始化过的segment实例
func newSegment(options *Options) *segment {
	return &segment{
		Data:    make(map[string]*values.Value, options.MapSizeOfSegment),
		Status:  NewStatus(),
		options: options,
		mutex:   &sync.RWMutex{},
	}
}

// 返回指定key数据
func (seg *segment) get(key string) (*values.Value, bool) {
	// 对当前segment加读锁
	seg.mutex.RLock()
	defer seg.mutex.RUnlock()
	// 获取从表中数据
	v, ok := seg.Data[key]
	if !ok {
		return nil, false
	}

	// 数据过期
	if !v.Alive() {
		// 加写锁
		seg.mutex.Lock()
		seg.delete(key)
		seg.mutex.Unlock()
		return nil, false
	}
	return v, true
}

// 将一个数据添加进segment
func (seg *segment) set(key string, data []byte, ttl int64, typ iface.Type) error {
	// 对当前segment进行加锁
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	// 检查是否以及存在
	if v, ok := seg.Data[key]; ok {
		seg.Status.subEntry(key, v.Data)
	}
	// 检查数据是否超出容量
	if !seg.checkEntryCapacity(key, data) {
		if ov, ok := seg.Data[key]; ok {
			seg.Status.addEntry(key, ov.Data)
		}

		// 超出单segment存储上限
		return errors.New("the entry size will exceed if you set this entry")
	}

	// 修改状态消息
	seg.Status.addEntry(key, data)
	seg.Data[key] = values.New(data, ttl, typ)
	return nil
}

// 从segment中删除指定key
func (seg *segment) delete(key string) {
	// 对当前segment加锁
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	if v, ok := seg.Data[key]; ok {
		seg.Status.subEntry(key, v.Data)
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
		if !v.Alive() {
			seg.Status.subEntry(k, v.Data)
			delete(seg.Data, k)
			count++
			if count >= seg.options.MaxGcCount {
				break
			}
		}
	}
}
