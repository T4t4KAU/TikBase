package bases

import (
	"TikBase/engine/data"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/dates/btree"
	"TikBase/pack/errorx"
	"TikBase/pack/utils"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Base 存储引擎
type Base struct {
	tree       *btree.Tree
	mutex      sync.RWMutex
	activeFile *data.File
	olderFiles map[uint32]*data.File
	options    Options
	fileIds    []int
}

func New() (*Base, error) {
	return NewBaseWith(DefaultOptions)
}

func NewBaseWith(options Options) (*Base, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	base := &Base{
		options:    options,
		olderFiles: make(map[uint32]*data.File),
		tree:       btree.New(),
	}

	if err := base.LoadDataFiles(); err != nil {
		return nil, err
	}

	if err := base.LoadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return base, nil
}

func (b *Base) Get(key string) (*values.Value, bool) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if len(key) == 0 {
		return nil, false
	}

	keyBytes := utils.StringToBytes(key)
	pos := b.tree.Get(keyBytes)
	if pos == nil {
		return nil, false
	}

	var dataFile *data.File
	if b.activeFile.FileId == pos.Fid {
		dataFile = b.activeFile
	} else {
		dataFile = b.olderFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, false
	}

	rec, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, false
	}

	if rec.Type == data.LogRecordDeleted {
		return nil, false
	}

	v := values.New(rec.Value, 0, iface.Type(rec.Type))
	return &v, true
}

func (b *Base) Set(key string, value iface.Value) bool {
	keyBytes := utils.StringToBytes(key)
	rec := data.LogRecord{
		Key:   keyBytes,
		Value: value.Bytes(),
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := b.AppendLogRecord(&rec)
	if err != nil {
		return false
	}

	if ok := b.tree.Put(keyBytes, pos); !ok {
		return false
	}
	return true
}

func (b *Base) SetBytes(key []byte, value iface.Value) bool {
	rec := data.LogRecord{
		Key:   key,
		Value: value.Bytes(),
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := b.AppendLogRecord(&rec)
	if err != nil {
		return false
	}

	if ok := b.tree.Put(key, pos); !ok {
		return false
	}
	return true
}

func (b *Base) Del(key string) bool {
	keyBytes := utils.StringToBytes(key)
	if pos := b.tree.Get(keyBytes); pos == nil {
		return false
	}

	if pos := b.tree.Get(keyBytes); pos == nil {
		return false
	}

	rec := &data.LogRecord{Key: keyBytes, Type: data.LogRecordDeleted}
	_, err := b.AppendLogRecord(rec)
	if err != nil {
		return false
	}

	ok := b.tree.Delete(keyBytes)
	if !ok {
		return false
	}
	return true
}

// 设置当前活跃文件
// 访问此方法前要持有互斥锁
func (b *Base) setActiveDataFile() error {
	var initFileId uint32 = 0

	if b.activeFile != nil {
		initFileId = b.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(b.options.DirPath, initFileId)
	if err != nil {
		return err
	}
	b.activeFile = dataFile
	return nil
}

func (b *Base) AppendLogRecord(rec *data.LogRecord) (*data.LogRecordPos, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// 判断当前活跃数据文件是否存在
	// 如果为空则初始化数据文件
	if b.activeFile == nil {
		if err := b.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(rec)

	// 如果写入的数据已经到达活跃文件的阈值 则关闭活跃文件 打开新文件
	if b.activeFile.WriteOff+size > b.options.DataFileSize {
		if err := b.activeFile.Sync(); err != nil {
			return nil, err
		}

		b.olderFiles[b.activeFile.FileId] = b.activeFile
		if err := b.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := b.activeFile.WriteOff
	if err := b.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if b.options.SyncWrites {
		if err := b.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	return &data.LogRecordPos{Fid: b.activeFile.FileId, Offset: writeOff}, nil
}

func (b *Base) LoadDataFiles() error {
	entries, err := os.ReadDir(b.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), data.FileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return errorx.ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	b.fileIds = fileIds

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(b.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			b.activeFile = dataFile
		} else {
			b.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// LoadIndexFromDataFiles 从数据文件中加载索引
// 遍历文件中所有记录 更新到内存索引中
func (b *Base) LoadIndexFromDataFiles() error {
	if len(b.fileIds) == 0 {
		return nil
	}

	for i, fid := range b.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.File
		if fileId == b.activeFile.FileId {
			dataFile = b.activeFile
		} else {
			dataFile = b.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			rec, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			pos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			var ok bool
			if rec.Type == data.LogRecordDeleted {
				ok = b.tree.Delete(rec.Key)
			} else {
				ok = b.tree.Put(rec.Key, pos)
			}
			if !ok {
				return errorx.ErrIndexUpdateFailed
			}

			offset += size
		}

		if i == len(b.fileIds)-1 {
			b.activeFile.WriteOff = offset
		}
	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
