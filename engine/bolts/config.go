package bolts

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/filter"
)

type Config struct {
	DirPath  string // 文件存放目录
	MaxLevel int    // 最大层数

	SSTSize             uint64              // SSTable大小
	SSTNumPerLevel      int                 // 每层SSTable的数量 默认10个
	SSTDataBlockSize    int                 // SSTable中Block大小
	SSTFooterSize       int                 // SSTable中footer部分大小
	Filter              iface.Filter        // 过滤器
	MemTableConstructor MemTableConstructor // memtable构造器
}

type ConfigOption func(*Config)

// WithMaxLevel lsm tree 最大层数. 默认为 7 层.
func WithMaxLevel(maxLevel int) ConfigOption {
	return func(c *Config) {
		c.MaxLevel = maxLevel
	}
}

func WithSSTSize(sstSize uint64) ConfigOption {
	return func(c *Config) {
		c.SSTSize = sstSize
	}
}

// WithSSTDataBlockSize sstable 中每个 block 块的大小限制. 默认为 16KB.
func WithSSTDataBlockSize(sstDataBlockSize int) ConfigOption {
	return func(c *Config) {
		c.SSTDataBlockSize = sstDataBlockSize
	}
}

// WithSSTNumPerLevel 每个 level 层预期最多存放的 sstable 文件个数. 默认为 10 个.
func WithSSTNumPerLevel(sstNumPerLevel int) ConfigOption {
	return func(c *Config) {
		c.SSTNumPerLevel = sstNumPerLevel
	}
}

// WithFilter 注入过滤器的具体实现. 默认使用本项目下实现的布隆过滤器 bloom filter.
func WithFilter(filter iface.Filter) ConfigOption {
	return func(c *Config) {
		c.Filter = filter
	}
}

// WithMemtableConstructor 注入有序表构造器
func WithMemtableConstructor(memtableConstructor MemTableConstructor) ConfigOption {
	return func(c *Config) {
		c.MemTableConstructor = memtableConstructor
	}
}

func repair(c *Config) {
	// lsm tree 默认为 7 层.
	if c.MaxLevel <= 1 {
		c.MaxLevel = 7
	}

	// level0 层每个 sstable 文件默认大小限制为 1MB.
	// 且每加深一层，sstable 文件大小限制阈值放大 10 倍.
	if c.SSTSize <= 0 {
		c.SSTSize = 1024 * 1024
	}

	// sstable 中每个 block 块的大小限制. 默认为 16KB.
	if c.SSTDataBlockSize <= 0 {
		c.SSTDataBlockSize = 16 * 1024 // 16KB
	}

	// 每个 level 层预期最多存放的 sstable 文件个数. 默认为 10 个.
	if c.SSTNumPerLevel <= 0 {
		c.SSTNumPerLevel = 10
	}

	// 注入过滤器的具体实现. 默认使用本项目下实现的布隆过滤器 bloom filter.
	if c.Filter == nil {
		c.Filter, _ = filter.NewBloomFilter(1024)
	}

	// 注入有序表构造器. 默认使用本项目下实现的跳表 skiplist.
	if c.MemTableConstructor == nil {
		// TODO:
		// c.MemTableConstructor = NewSkiplist
	}
}
