package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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
	Size   uint32 // 标识数据在磁盘中大小
}

type TxRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
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

// EncodeLogRecord 编码日志记录
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

func getLogRecordCRC(rec *LogRecord, header []byte) uint32 {
	if rec == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, rec.Key)
	crc = crc32.Update(crc, crc32.IEEETable, rec.Value)

	return crc
}

// EncodeLogRecordPos 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)

	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// DecodeLogRecordPos 对位置信息进行解码
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{Fid: uint32(fileId), Offset: offset}
}
