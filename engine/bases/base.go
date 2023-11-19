package bases

import (
	"TikBase/engine/data"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/dates/btree"
	"TikBase/pack/errorx"
	"TikBase/pack/utils"
	"errors"
	"github.com/gofrs/flock"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Base 存储引擎
type Base struct {
	index      *btree.Tree
	mutex      sync.RWMutex
	activeFile *data.File
	olderFiles map[uint32]*data.File
	options    Options
	fileIds    []int        // 加载索引时使用
	fileLock   *flock.Flock // 文件锁
	seqNo      uint64
}

func New() (*Base, error) {
	return NewBaseWith(DefaultOptions)
}

// NewBaseWith 启动存储引擎
func NewBaseWith(options Options) (*Base, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 检查目录是否存在 创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化数据结构
	base := &Base{
		options:    options,
		olderFiles: make(map[uint32]*data.File),
		index:      btree.New(),
	}

	// 加载数据文件
	if err := base.LoadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件加载索引
	if err := base.LoadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return base, nil
}

// Get 读取数据
func (b *Base) Get(key string) (*values.Value, bool) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if len(key) == 0 {
		return nil, false
	}

	keyBytes := utils.StringToBytes(key)

	// 从索引中获取key位置
	pos := b.index.Get(keyBytes)
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

	// 读取日志记录
	rec, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, false
	}

	if rec.Type == data.LogRecordDeleted {
		return nil, false
	}

	v := values.New(rec.Value, 0, iface.STRING)
	return &v, true
}

func (b *Base) Set(key string, value iface.Value) bool {
	keyBytes := utils.StringToBytes(key)
	rec := data.LogRecord{
		Key:   logRecordKeyWithSeqNo(keyBytes, nonTransactionSeqNo),
		Value: value.Bytes(),
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := b.AppendLogRecordWithLock(&rec)
	if err != nil {
		return false
	}

	// 更新索引
	if ok := b.index.Put(keyBytes, pos); !ok {
		return false
	}
	return true
}

func (b *Base) SetBytes(key []byte, value iface.Value) bool {
	rec := data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Value: value.Bytes(),
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := b.AppendLogRecordWithLock(&rec)
	if err != nil {
		return false
	}

	if ok := b.index.Put(key, pos); !ok {
		return false
	}
	return true
}

func (b *Base) Del(key string) bool {
	keyBytes := utils.StringToBytes(key)

	// 从索引中检查key是否存在
	if pos := b.index.Get(keyBytes); pos == nil {
		return false
	}

	// 构造LogRecord 标记墓碑值
	rec := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(keyBytes, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	_, err := b.AppendLogRecordWithLock(rec)
	if err != nil {
		return false
	}

	// 从内存索引中将对应key删除
	ok := b.index.Delete(keyBytes)
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

	// 打开数据文件
	dataFile, err := data.OpenDataFile(b.options.DirPath, initFileId)
	if err != nil {
		return err
	}
	b.activeFile = dataFile
	return nil
}

// AppendLogRecord 追加日志记录
func (b *Base) AppendLogRecord(rec *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在 如果为空则初始化数据文件
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
	// 读取目录项 找到以.data结尾的文件
	entries, err := os.ReadDir(b.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// 遍历目录中所有文件
	for _, entry := range entries {
		// 匹配后缀为.data
		if strings.HasSuffix(entry.Name(), data.FileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return errorx.ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件ID列表进行排序
	sort.Ints(fileIds)
	b.fileIds = fileIds

	// 遍历每个文件ID 打开对应数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(b.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// id最大的文件即当前活跃文件
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

	// 更新索引信息
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = b.index.Delete(key)
		} else {
			ok = b.index.Put(key, pos)
		}

		if !ok {
			panic(errorx.ErrIndexUpdateFailed)
		}
	}

	// 当前序列号
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 暂存事务数据 事务ID -> 列表
	txRecords := make(map[uint64][]*data.TxRecord)

	// 遍历文件ID列表
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
			// 读取日志记录
			rec, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造索引信息
			pos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析key 获取事务序列号
			realKey, seqNo := parseLogRecordKey(rec.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作 直接更新内存索引
				updateIndex(realKey, rec.Type, pos)
			} else {
				// 已完成事务 更新到内存索引中
				if rec.Type == data.LogRecordTxnFinished {
					for _, txRecord := range txRecords[seqNo] {
						updateIndex(txRecord.Record.Key, txRecord.Record.Type, txRecord.Pos)
					}
					delete(txRecords, seqNo)
				} else {
					rec.Key = realKey

					// 暂存数据
					txRecords[seqNo] = append(txRecords[seqNo], &data.TxRecord{
						Record: rec,
						Pos:    pos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增offset 下次从新位置开始读取
			offset += size
		}

		// 如果当前为活跃文件 更新该文件写偏移
		if i == len(b.fileIds)-1 {
			b.activeFile.WriteOff = offset
		}
	}

	// 更新当前最新序列号
	b.seqNo = currentSeqNo

	return nil
}

func (b *Base) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.File
	if b.activeFile.FileId == pos.Fid {
		dataFile = b.activeFile
	} else {
		dataFile = b.olderFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, errorx.ErrDataFileNotFound
	}

	rec, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if rec.Type == data.LogRecordDeleted {
		return nil, errorx.ErrKeyNotFound
	}

	return rec.Value, nil
}

func (b *Base) Close() error {
	if b.activeFile == nil {
		return nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if err := b.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range b.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// ListKeys 获取所有Key
func (b *Base) ListKeys() [][]byte {
	it := b.index.Iterator(false)
	keys := make([][]byte, b.index.Size())

	var idx int
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}

	return keys
}

func (b *Base) Fold(fn func(key []byte, value []byte) bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	it := b.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		val, err := b.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		if !fn(it.Key(), val) {
			break
		}
	}

	return nil
}

func (b *Base) AppendLogRecordWithLock(rec *data.LogRecord) (*data.LogRecordPos, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.AppendLogRecord(rec)
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
