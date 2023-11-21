package bases

import (
	"TikBase/engine/data"
	"TikBase/pack/errorx"
	"TikBase/pack/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

// Merge 合并
func (b *Base) Merge() error {
	if b.activeFile == nil {
		return nil
	}

	b.mutex.Lock()

	// 判断是否已经有merge在进行
	// 如果merge正在进行 那么退出当前merge
	if b.merging {
		b.mutex.Unlock()
		return errorx.ErrMergeIsProgress
	}

	// 标记正在merge
	b.merging = true
	defer func() {
		b.merging = false
	}()

	// 持久化当前活跃文件
	if err := b.activeFile.Sync(); err != nil {
		b.mutex.Unlock()
		return err
	}

	nonMergeFileId := b.activeFile.FileId + 1        // merge后新文件ID
	b.olderFiles[b.activeFile.FileId] = b.activeFile // 归入旧文件

	// 打开并设置新的活跃文件
	if err := b.setActiveDataFile(); err != nil {
		b.mutex.Unlock()
		return nil
	}

	// 将旧文件归入待合并文件
	var mergeFiles []*data.File
	for _, file := range b.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	b.mutex.Unlock()

	// 将待merge文件排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// 初始化临时目录 用于merge
	mergePath := b.getMergePath()
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 初始化merge选项
	mergeOptions := b.options
	mergeOptions.SyncWrites = false
	mergeOptions.DirPath = mergePath
	mergeDB, err := NewBaseWith(mergeOptions)
	if err != nil {
		return err
	}

	// 打开Hint文件 文件存储索引
	hintFile, err := data.OpenHintFile(mergeDB.options.DirPath)
	if err != nil {
		return err
	}

	// 遍历待merge文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			// 读取日志记录
			rec, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 解析日志记录key
			realKey, _ := parseLogRecordKey(rec.Key)
			pos := b.index.Get(realKey)

			// 基于索引中的key 判断是否为有效的记录
			// 记录有效 则追加日志记录
			if pos != nil && pos.Fid == dataFile.FileId && pos.Offset == offset {
				rec.Key = LogRecordKeyWithSeqNo(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.AppendLogRecord(rec) // 向临时数据库追加日志记录
				if err != nil {
					return err
				}

				// 将当前位置索引写入Hint文件
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}

			offset += size
		}
	}

	// 将数据持久化到磁盘
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 标识文件 表示merge已经完成
	mergeFinFile, err := data.OpenMergeFinishedFile(mergeDB.options.DirPath)
	if err != nil {
		return err
	}

	mergeFinRecord := &data.LogRecord{
		Key:   utils.S2B(mergeFinishedKey),
		Value: utils.S2B(strconv.Itoa(int(nonMergeFileId))),
	}

	// 将finish记录编码并写入finish文件
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}

	// 将merge finish文件持久化
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 获取合并路径
func (b *Base) getMergePath() string {
	dir := path.Dir(path.Clean(b.options.DirPath))
	base := path.Base(b.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (b *Base) logMergeFiles() error {
	mergePath := b.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	var mergeFinished bool
	var fileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		fileNames = append(fileNames, entry.Name())
	}

	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := b.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(b.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	for _, fileName := range fileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(b.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

// LoadIndexFromHintFile 从Hint文件中加载索引
func (b *Base) LoadIndexFromHintFile() error {
	// 查看hint索引文件是否存在
	hintFileName := filepath.Join(b.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开Hint索引文件
	hintFile, err := data.OpenHintFile(b.options.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		// 读取日志记录
		rec, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 获取日志记录位置索引
		pos := data.DecodeLogRecordPos(rec.Value)
		b.index.Put(rec.Key, pos)
		offset += size
	}

	return nil
}

func (b *Base) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	rec, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(utils.B2S(rec.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

// LoadMergeFiles 加载merge数据目录
func (b *Base) LoadMergeFiles() error {
	// 如果merge目录存在则加载
	mergePath := b.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		// 删除merge数据目录
		_ = os.RemoveAll(mergePath)
	}()

	// 读取目录中的文件项
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	var mergeFinished bool
	var fileNames []string

	// 查找标识merge完成的文件
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		fileNames = append(fileNames, entry.Name())
	}

	// merge未完成则退出
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := b.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	var fileId uint32 = 0

	// 移除旧文件
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(b.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 移动新文件
	for _, fileName := range fileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(b.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}
