package data

import (
	"TikBase/pack/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const FileNameSuffix = ".data"

type File struct {
	FileId    uint32
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fid uint32) (*File, error) {
	name := filepath.Join(dirPath, fmt.Sprintf("%09d", fid)+FileNameSuffix)
	iom, err := fio.NewIOManager(name)
	if err != nil {
		return nil, err
	}
	return &File{
		FileId:    fid,
		WriteOff:  0,
		IOManager: iom,
	}, nil
}

func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	size, err := f.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize

	// 读取长度超过文件长度
	if offset+maxLogRecordHeaderSize > size {
		headerBytes = size - offset
	}

	// 读取 Header
	headerBuf, err := f.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解析 Header
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
		kvBuf, err := f.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		rec.Key = kvBuf[:keySize]
		rec.Value = kvBuf[keySize:]
	}

	crc := getLogRecordCRC(rec, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return rec, recordSize, nil
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
