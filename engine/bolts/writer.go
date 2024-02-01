package bolts

import (
	"bytes"
	"encoding/binary"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"os"
	"path"
)

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
}

func NewSSTWriter(file string, conf *Config) (*SSTWriter, error) {
	dest, err := os.OpenFile(path.Join(conf.DirPath, file), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTWriter{
		conf:          conf,
		dest:          dest,
		dataBuffer:    bytes.NewBuffer([]byte{}),
		filterBuffer:  bytes.NewBuffer([]byte{}),
		indexBuffer:   bytes.NewBuffer([]byte{}),
		blockToFilter: make(map[uint64][]byte),
	}, nil
}

func (w *SSTWriter) Finish() (size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	w.refreshBlock()
	w.insertIndex(w.prevKey)

	_, _ = w.filterBlock.FlushTo(w.filterBuffer)
	_, _ = w.indexBlock.FlushTo(w.indexBuffer)

	footer := make([]byte, w.conf.SSTFooterSize)
	size = uint64(w.dataBuffer.Len())
	n := binary.PutUvarint(footer[0:], size)
	filterBuffLen := uint64(w.filterBuffer.Len())
	n += binary.PutUvarint(footer[n:], filterBuffLen)

	size += filterBuffLen

	indexBufferLen := uint64(w.indexBuffer.Len())
	n += binary.PutUvarint(footer[n:], indexBufferLen)
	size += indexBufferLen

	_, _ = w.dest.Write(w.dataBuffer.Bytes())
	_, _ = w.dest.Write(w.filterBuffer.Bytes())
	_, _ = w.dest.Write(w.indexBuffer.Bytes())
	_, _ = w.dest.Write(footer)

	blockToFilter = w.blockToFilter
	index = w.index
	return
}

func (w *SSTWriter) refreshBlock() {
	if w.conf.Filter.KeyLen() == 0 {
		return
	}

	w.prevBlockOffset = uint64(w.dataBuffer.Len())
	filterBitmap := w.conf.Filter.Hash()
	w.blockToFilter[w.prevBlockOffset] = filterBitmap
	n := binary.PutUvarint(w.assistScratch[0:], w.prevBlockOffset)
	w.filterBlock.Append(w.assistScratch[:n], filterBitmap)

	w.conf.Filter.Reset()
	w.prevBlockSize, _ = w.dataBlock.FlushTo(w.dataBuffer)
}

func (w *SSTWriter) insertIndex(key []byte) {
	indexKey := utils.GetSeparatorBetween(w.prevKey, key)
	n := binary.PutUvarint(w.assistScratch[0:], w.prevBlockOffset)
	n += binary.PutUvarint(w.assistScratch[n:], w.prevBlockSize)

	w.indexBlock.Append(indexKey, w.assistScratch[:n])
	w.index = append(w.index, &Index{
		Key:             indexKey,
		PrevBlockOffset: w.prevBlockOffset,
		PrevBlockSize:   w.prevBlockSize,
	})
}

func (w *SSTWriter) Close() {
	_ = w.dest.Close()
	w.dataBuffer.Reset()
	w.indexBuffer.Reset()
	w.filterBuffer.Reset()
}
