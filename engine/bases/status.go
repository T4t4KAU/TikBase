package bases

import "github.com/T4t4KAU/TikBase/pkg/utils"

// Status 状态信息
type Status struct {
	keyNum          uint  // key的数量
	DataFileNum     uint  // 数据文件个数
	ReclaimableSize int64 // 数据可回收的空间 字节为单位
	DiskSize        int64 // 所占磁盘空间大小
}

func (st *Status) KeyCount() uint {
	return st.keyNum
}

// Status 返回数据库统计信息
func (b *Base) Status() *Status {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	var dataFilesNum = uint(len(b.olderFiles))
	if b.activeFile != nil {
		dataFilesNum++
	}
	dirSize, err := utils.DirSize(b.options.DirPath)
	if err != nil {
		panic(err)
	}

	return &Status{
		keyNum:          uint(b.index.Size()),
		DataFileNum:     dataFilesNum,
		ReclaimableSize: b.reclaimableSize,
		DiskSize:        dirSize,
	}
}
