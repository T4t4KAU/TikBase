package caches

import (
	"bytes"
	"encoding/gob"
	"github.com/T4t4KAU/TikBase/iface"
	"sync"
	"sync/atomic"
	"time"
)

// Cache 代表缓存结构体
type Cache struct {
	segmentSize int       // segment数量
	segments    []segment // 存储segment实例
	options     *Options  // 缓存配置
	dumping     int32     // 标识当前缓存是否处于持久化状态 处于持久化状态则所有更新操作自旋
}

// New 返回默认配置的缓存对象
func New() (*Cache, error) {
	return NewCacheWith(DefaultOptions())
}

// NewCacheWith 返回一个指定配置的缓存对象
func NewCacheWith(options Options) (*Cache, error) {
	if cache, ok := recoverFromDumpFile(options.DumpFile); ok {
		return cache, nil
	}
	return &Cache{
		segmentSize: options.SegmentSize,
		segments:    newSegments(options), // 初始化所有segment
		options:     &options,
		dumping:     0,
	}, nil
}

// 创建segment
func newSegments(options Options) []segment {
	segments := make([]segment, options.SegmentSize)
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
	return &c.segments[index(key)&(c.segmentSize-1)]
}

// Get 返回指定value 未找到则返回false
func (c *Cache) Get(key string) (iface.Value, error) {
	c.waitForDumping()
	return c.segmentOf(key).get(key)
}

func (c *Cache) Set(key string, value []byte, ttl int64) error {
	return c.SetWithTTL(key, value, ttl, iface.STRING)
}

// Del 从缓存中删除指定键值对
func (c *Cache) Del(key string) error {
	c.waitForDumping()
	return c.segmentOf(key).delete(key)
}

// Expire 设置超时时间
func (c *Cache) Expire(key string, ttl int64) error {
	c.waitForDumping()
	val, err := c.segmentOf(key).get(key)
	if err != nil {
		return err
	}
	val.TTL = ttl
	return nil
}

func (c *Cache) Keys() [][]byte {
	c.waitForDumping()

	keys := make([][]byte, 0)
	for _, seg := range c.segments {
		for key := range seg.Data {
			keys = append(keys, []byte(key))
		}
	}
	return keys
}

// Exist 检查键是否存在
func (c *Cache) Exist(key string) error {
	c.waitForDumping()
	_, err := c.segmentOf(key).get(key)
	return err
}

// SetWithTTL 添加到指定的数据到缓存中 设置相应有效期
func (c *Cache) SetWithTTL(key string, value []byte, ttl int64, typ iface.Type) error {
	c.waitForDumping()
	return c.segmentOf(key).set(key, value, ttl, typ)
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
	return result
}

// 清理缓存中过期数据
func (c *Cache) gc() {
	c.waitForDumping()
	wg := &sync.WaitGroup{}
	for _, seg := range c.segments {
		wg.Add(1)
		go func(s segment) {
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
			_ = c.dump()
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

func (c *Cache) RecoverFromBytes(data []byte) error {
	d := newEmptyDump()
	buffer := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buffer).Decode(d); err != nil {
		return nil
	}

	// 初始化对象
	for _, seg := range *d.Segments {
		seg.options = *d.Options
		seg.mutex = &sync.RWMutex{}
	}

	c.segmentSize = d.SegmentSize
	c.segments = *d.Segments
	c.options = d.Options
	c.dumping = 0

	return nil
}

// 从dump文件中恢复缓存
func recoverFromDumpFile(dumpFile string) (*Cache, bool) {
	cache, err := newEmptyDump().from(dumpFile)
	if err != nil {
		return nil, false
	}
	return cache, true
}
