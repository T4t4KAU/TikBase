package bolts

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"io"
	"os"
	"path"
	"sync"
)

type Tree struct {
	conf *Config

	options    Options
	dataLock   sync.RWMutex
	levelLocks []sync.RWMutex

	memTable iface.MemTable

	rdOnlyMemTable []*MemTableCompactItem
}

type KVPair struct {
	Key   []byte
	Value []byte
}

type Config struct {
	DirPath  string
	MaxLevel int

	SSTSize          uint64
	SSTNumPerLevel   int
	SSTDataBlockSize int
	SSTFooterSize    int
	filter           iface.Filter
}

type SSTReader struct {
	src          *os.File
	reader       *bufio.Reader
	filterOffset uint64
	filterSize   uint64
	indexOffset  uint64
	indexSize    uint64

	footerSize int
}

type SSTWriter struct {
	conf          *Config
	dest          *os.File
	dataBuffer    *bytes.Buffer
	filterBuffer  *bytes.Buffer
	indexBuffer   *bytes.Buffer
	blockToFilter map[uint64][]byte
	index         []*Index

	dataBlock       *Block
	filterBlock     *Block
	indexBlock      *Block
	assistScratch   [20]byte
	prevKey         []byte
	prevBlockOffset uint64
	prevBlockSize   uint64

	filter iface.Filter
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

func (r *SSTWriter) Finish() (size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	r.refreshBlock()
	r.insertIndex(r.prevKey)

	_, _ = r.filterBlock.FlushTo(r.filterBuffer)
	_, _ = r.indexBlock.FlushTo(r.indexBuffer)

	footer := make([]byte, r.conf.SSTFooterSize)
	size = uint64(r.dataBuffer.Len())
	n := binary.PutUvarint(footer[0:], size)
	filterBuffLen := uint64(r.filterBuffer.Len())
	n += binary.PutUvarint(footer[n:], filterBuffLen)

	size += filterBuffLen

	indexBufferLen := uint64(r.indexBuffer.Len())
	n += binary.PutUvarint(footer[n:], indexBufferLen)
	size += indexBufferLen

	_, _ = r.dest.Write(r.dataBuffer.Bytes())
	_, _ = r.dest.Write(r.filterBuffer.Bytes())
	_, _ = r.dest.Write(r.indexBuffer.Bytes())
	_, _ = r.dest.Write(footer)

	blockToFilter = r.blockToFilter
	index = r.index
	return
}

func (r *SSTWriter) refreshBlock() {
	if r.filter.KeyLen() == 0 {
		return
	}

	r.prevBlockOffset = uint64(r.dataBuffer.Len())
	filterBitmap := r.filter.Hash()
	r.blockToFilter[r.prevBlockOffset] = filterBitmap
	n := binary.PutUvarint(r.assistScratch[0:], r.prevBlockOffset)
	r.filterBlock.Append(r.assistScratch[:n], filterBitmap)

	r.filter.Reset()
	r.prevBlockSize, _ = r.dataBlock.FlushTo(r.dataBuffer)
}

func (r *SSTWriter) insertIndex(key []byte) {
	indexKey := utils.GetSeparatorBetween(r.prevKey, key)
	n := binary.PutUvarint(r.assistScratch[0:], r.prevBlockOffset)
	n += binary.PutUvarint(r.assistScratch[n:], r.prevBlockSize)

	r.indexBlock.Append(indexKey, r.assistScratch[:n])
	r.index = append(r.index, &Index{
		Key:             indexKey,
		PrevBlockOffset: r.prevBlockOffset,
		PrevBlockSize:   r.prevBlockSize,
	})
}
