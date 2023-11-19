package bases

import (
	"os"
)

const (
	BT IndexerType = iota + 1

	// ART  自适应基数树索引
	ART
)

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 索引类型
	IndexType IndexerType
}

type IndexerType = int8

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BT,
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum uint // 一个批次当中最大的数据量
	SyncWriters bool // 提交时是否Sync持久化
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWriters: true,
}
