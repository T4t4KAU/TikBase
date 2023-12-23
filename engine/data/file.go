package data

import (
	"errors"
	"fmt"
	"github.com/T4t4KAU/TikBase/pack/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	FileNameSuffix        = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

// File 文件管理结构
type File struct {
	FileId    uint32        // 文件编号
	WriteOff  int64         //文件写偏移 记录文件写入位置
	IOManager fio.IOManager // 文件IO管理器 文件操作接口
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fid uint32, ioType fio.FileIOType) (*File, error) {
	// 拼接文件路径
	name := filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+FileNameSuffix)
	iom, err := fio.NewIOManager(name, ioType) // 初始化文件IO管理器
	if err != nil {
		return nil, err
	}

	// 返回文件结构
	return &File{
		FileId:    fid,
		WriteOff:  0,
		IOManager: iom,
	}, nil
}

// OpenHintFile 打开Hint索引文件
func OpenHintFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开标识merge完成的文件
func OpenMergeFinishedFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 储存事务序列号的文件
func OpenSeqNoFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// ReadLogRecord 从数据文件读取LogRecord
func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	size, err := f.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大header长度已经超过文件长度 读取到文件的末尾
	var headerBytes int64 = maxLogRecordHeaderSize

	// 读取长度超过文件长度
	if offset+maxLogRecordHeaderSize > size {
		headerBytes = size - offset
	}

	// 读取 Header 信息
	headerBuf, err := f.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解析 Header 信息
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应 key 和 value 长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	rec := &LogRecord{Type: header.recordType}

	// 开始读取用户实际存储的 KV 数据
	if keySize > 0 || valueSize > 0 {
		// 偏移headerSize后 读取KV数据
		kvBuf, err := f.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		rec.Key = kvBuf[:keySize]   // key
		rec.Value = kvBuf[keySize:] // value
	}

	// 获取CRC校验值 校验数据时效性
	crc := getLogRecordCRC(rec, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return rec, recordSize, nil
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*File, error) {
	iom, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &File{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: iom,
	}, nil
}

func (f *File) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	hintRecord := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}

	encRecord, _ := EncodeLogRecord(hintRecord)
	return f.Write(encRecord)
}

func (f *File) Write(buf []byte) error {
	n, err := f.IOManager.Write(buf)
	if err != nil {
		return err
	}
	f.WriteOff += int64(n)
	return nil
}

func (f *File) Sync() error {
	// 将活跃文件数据刷到磁盘
	return f.IOManager.Sync()
}

func (f *File) Close() error {
	return f.IOManager.Close()
}

func (f *File) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := f.IOManager.Read(b, offset)
	return b, err
}

// SetIOManager 设置新的IO管理器
func (f *File) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := f.IOManager.Close(); err != nil {
		return err
	}
	iom, err := fio.NewIOManager(GetDataFileName(dirPath, f.FileId), ioType)
	if err != nil {
		return err
	}
	f.IOManager = iom
	return nil
}
