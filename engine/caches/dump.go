package caches

import (
	"encoding/gob"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"os"
	"sync"
)

// 持久化结构体
type dump struct {
	SegmentSize int
	Segments    *[]segment
	Options     *Options
}

// 返回空持久化实例
func newEmptyDump() *dump {
	return &dump{}
}

// 返回一个从缓存实例初始化过来的持久化实例
func newDump(c *Cache) *dump {
	return &dump{
		SegmentSize: c.segmentSize,
		Segments:    &c.segments,
		Options:     c.options,
	}
}

// 将dump实例持久化文件中
func (d *dump) to(dumpFile string) error {
	newDumpFile := dumpFile + utils.NowSuffix()
	file, err := os.OpenFile(newDumpFile,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// 使用Gob编码
	err = gob.NewEncoder(file).Encode(d)
	if err != nil {
		_ = os.Remove(newDumpFile)
		return err
	}

	// 删除持久化文件
	_ = os.Remove(dumpFile)
	return os.Rename(newDumpFile, dumpFile)
}

// 从dump文件中恢复cache结构对象
func (d *dump) from(dumpFile string) (*Cache, error) {
	file, err := os.Open(dumpFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// 创建解码器
	if err = gob.NewDecoder(file).Decode(d); err != nil {
		return nil, err
	}

	// 初始化对象
	for _, seg := range *d.Segments {
		seg.options = *d.Options
		seg.mutex = &sync.RWMutex{}
	}
	return &Cache{
		segmentSize: d.SegmentSize,
		segments:    *d.Segments,
		options:     d.Options,
		dumping:     0,
	}, nil
}
