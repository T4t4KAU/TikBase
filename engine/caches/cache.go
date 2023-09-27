package caches

import (
	"sync"
	"sync/atomic"
	"time"
)

// Cache 代表缓存结构体
type Cache struct {
	segmentSize int        // segment数量
	segments    []*segment // 存储segment实例
	options     *Options   // 缓存配置
	dumping     int32      // 标识当前缓存是否处于持久化状态 处于持久化状态则所有更新操作自旋
}

// NewCache 返回默认配置的缓存对象
func NewCache() *Cache {
	return NewCacheWith(DefaultOptions())
}

// NewCacheWith 返回一个指定配置的缓存对象
func NewCacheWith(options Options) *Cache {
	if cache, ok := recoverFromDumpFile(options.DumpFile); ok {
		return cache
	}
	return &Cache{
		segmentSize: options.SegmentSize,
		segments:    newSegments(&options), // 初始化所有segment
		options:     &options,
		dumping:     0,
	}
}

// 创建segment
func newSegments(options *Options) []*segment {
	segments := make([]*segment, options.SegmentSize)
	for i := 0; i < options.SegmentSize; i++ {
		segments[i] = newSegment(options)
	}
	return segments
}

// segment选择算法
func index(key string) int {
	idx := 0
	keyBytes := []byte(key)
	for _, b := range keyBytes {
		idx = 31*idx + int(b&0xff)
	}
	// 生成哈希值
	return idx ^ (idx >> 16)
}

// 返回key对应的segment
func (c *Cache) segmentOf(key string) *segment {
	return c.segments[index(key)&(c.segmentSize-1)]
}

// 从dump文件中恢复缓存
func recoverFromDumpFile(dumpFile string) (*Cache, bool) {
	cache, err := newEmptyDump().from(dumpFile)
	if err != nil {
		return nil, false
	}
	return cache, true
}

// Get 返回指定value 未找到则返回false
func (c *Cache) Get(key string) ([]byte, bool) {
	c.waitForDumping()
	return c.segmentOf(key).get(key)
}

// Set 保存键值对到缓存
func (c *Cache) Set(key string, value []byte) error {
	return c.SetWithTTL(key, value, NeverExpire)
}

// SetWithTTL 添加到指定的数据到缓存中 设置相应有效期
func (c *Cache) SetWithTTL(key string, value []byte, ttl int64) error {
	c.waitForDumping()
	return c.segmentOf(key).set(key, value, ttl)
}

// Delete 从缓存中删除指定键值对
func (c *Cache) Delete(key string) error {
	c.waitForDumping()
	c.segmentOf(key).delete(key)
	return nil
}

// Status 返回缓存当前状态
func (c *Cache) Status() Status {
	result := NewStatus()
	for _, seg := range c.segments {
		status := seg.status()
		result.Count += status.Count
		result.KeySize += status.KeySize
		result.ValueSize += status.ValueSize
	}
	return *result
}

// 清理缓存中过期数据
func (c *Cache) gc() {
	c.waitForDumping()
	wg := &sync.WaitGroup{}
	for _, seg := range c.segments {
		wg.Add(1)
		go func(s *segment) {
			defer wg.Done()
			s.gc()
		}(seg)
	}
	wg.Wait()
}

// AutoGC 开启异步协程定时清理过期数据
func (c *Cache) AutoGC() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.options.GcDuration) * time.Minute)
		for range ticker.C {
			c.gc()
		}
	}()
}

// 将缓存数据持久化到文件中
func (c *Cache) dump() error {
	// 设置持久化标识为1
	atomic.StoreInt32(&c.dumping, 1)
	defer atomic.StoreInt32(&c.dumping, 0)
	return newDump(c).to(c.options.DumpFile)
}

// AutoDump 开启异步协程定时持久化缓存数据
func (c *Cache) AutoDump() {
	go func() {
		d := time.Duration(c.options.DumpDuration) * time.Minute
		ticker := time.NewTicker(d)
		for range ticker.C {
			c.dump()
		}
	}()
}

// 等待持久化完成
func (c *Cache) waitForDumping() {
	for atomic.LoadInt32(&c.dumping) != 0 {
		// 每次循环等待一定时间
		time.Sleep(time.Duration(c.options.CasSleepTime) * time.Microsecond)
	}
}
