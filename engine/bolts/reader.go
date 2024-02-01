package bolts

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
)

type SSTReader struct {
	conf         *Config       // 配置文件
	src          *os.File      // 对应文件
	reader       *bufio.Reader // 文件读取reader
	filterOffset uint64        // 块起始位置
	filterSize   uint64        // 块大小
	indexOffset  uint64        // 索引块起始位置
	indexSize    uint64        // 索引块大小

	footerSize int
}

func NewSSTReader(file, dirPath string) (*SSTReader, error) {
	src, err := os.OpenFile(path.Join(dirPath, file), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTReader{
		src:    src,
		reader: bufio.NewReader(src),
	}, nil
}

func (r *SSTReader) Size() (uint64, error) {
	if r.indexOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return 0, err
		}
	}
	return r.indexOffset + r.indexSize, nil
}

func (r *SSTReader) Close() {
	r.reader.Reset(r.src)
	_ = r.src.Close()
}

func (r *SSTReader) ReadFooter() error {
	if _, err := r.src.Seek(-int64(r.footerSize), io.SeekEnd); err != nil {
		return err
	}

	r.reader.Reset(r.src)

	var err error
	if r.filterOffset, err = binary.ReadUvarint(r.reader); err != nil {
		return err
	}

	if r.filterSize, err = binary.ReadUvarint(r.reader); err != nil {
		return err
	}

	if r.indexOffset, err = binary.ReadUvarint(r.reader); err != nil {
		return err
	}

	if r.indexSize, err = binary.ReadUvarint(r.reader); err != nil {
		return err
	}

	return nil
}

func (r *SSTReader) ReadFilter() (map[uint64][]byte, error) {
	if r.filterOffset == 0 || r.filterSize == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	filterBlock, err := r.ReadBlock(r.filterOffset, r.filterSize)
	if err != nil {
		return nil, err
	}

	return r.readFilter(filterBlock)
}

func (r *SSTReader) ReadBlock(offset, size uint64) ([]byte, error) {
	if _, err := r.src.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	r.reader.Reset(r.src)
	b := make([]byte, size)
	_, err := io.ReadFull(r.reader, b)
	return b, err
}

func (r *SSTReader) readFilter(b []byte) (map[uint64][]byte, error) {
	blockToFilter := make(map[uint64][]byte)
	buffer := bytes.NewBuffer(b)

	var preKey []byte
	for {
		key, value, err := r.ReadRecord(preKey, buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		blockOffset, _ := binary.Uvarint(key)
		blockToFilter[blockOffset] = value
		preKey = key
	}

	return blockToFilter, nil
}

func (r *SSTReader) readIndex(b []byte) ([]*Index, error) {
	var (
		index   []*Index
		prevKey []byte
	)

	buffer := bytes.NewBuffer(b)
	for {
		key, value, err := r.ReadRecord(prevKey, buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		blockOffset, n := binary.Uvarint(value)
		blockSize, _ := binary.Uvarint(value[n:])
		index = append(index, &Index{
			Key:             prevKey,
			PrevBlockOffset: blockOffset,
			PrevBlockSize:   blockSize,
		})

		prevKey = key
	}

	return index, nil
}

func (r *SSTReader) ReadBlockData(b []byte) ([]*KVPair, error) {
	var prevKey []byte
	var data []*KVPair

	buffer := bytes.NewBuffer(b)

	for {
		key, value, err := r.ReadRecord(prevKey, buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		data = append(data, &KVPair{
			Key:   prevKey,
			Value: value,
		})

		prevKey = key
	}

	return data, nil
}

func (r *SSTReader) ReadIndex() ([]*Index, error) {
	if r.indexOffset == 0 || r.indexSize == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	indexBlock, err := r.ReadBlock(r.indexOffset, r.indexSize)
	if err != nil {
		return nil, err
	}

	return r.readIndex(indexBlock)
}

func (r *SSTReader) ReadData() ([]*KVPair, error) {
	if r.indexOffset == 0 || r.indexSize == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	dataBlock, err := r.ReadBlock(0, r.filterOffset)
	if err != nil {
		return nil, err
	}

	return r.ReadBlockData(dataBlock)
}

func (r *SSTReader) ReadRecord(prevKey []byte, buffer *bytes.Buffer) ([]byte, []byte, error) {
	sharedPrefixLen, err := binary.ReadUvarint(buffer)
	if err != nil {
		return nil, nil, err
	}

	keyLen, err := binary.ReadUvarint(buffer)
	if err != nil {
		return nil, nil, err
	}

	valLen, err := binary.ReadUvarint(buffer)
	if err != nil {
		return nil, nil, err
	}

	key := make([]byte, keyLen)
	if _, err = io.ReadFull(buffer, key); err != nil {
		return nil, nil, err
	}

	value := make([]byte, valLen)
	if _, err = io.ReadFull(buffer, value); err != nil {
		return nil, nil, err
	}

	sharedPrefix := make([]byte, sharedPrefixLen)
	copy(sharedPrefix, prevKey[:sharedPrefixLen])
	key = append(sharedPrefix, key...)

	return key, value, nil
}
