package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

func EncodeLogRecord(rec *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = rec.Type

	var index = 5

	index += binary.PutVarint(header[index:], int64(len(rec.Key)))
	index += binary.PutVarint(header[index:], int64(len(rec.Value)))

	var size = index + len(rec.Key) + len(rec.Value)
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])

	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], rec.Key)
	copy(encBytes[index+len(rec.Key):], rec.Value)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// DecodeLogRecordHeader 对字节数组中的 Header 信息进行解码
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5

	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}
