package bolts

import "bytes"

type Node struct {
	conf          *Config
	file          string
	level         int
	seq           int32
	size          uint64
	blockToFilter map[uint64][]byte

	index     []*Index
	startKey  []byte
	endKey    []byte
	sstReader *SSTReader
}

func NewNode(conf *Config, file string, reader *SSTReader, level int,
	seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) *Node {
	return &Node{
		conf:          conf,
		file:          file,
		sstReader:     reader,
		level:         level,
		seq:           seq,
		size:          size,
		blockToFilter: blockToFilter,
		index:         index,
		startKey:      index[0].Key,
		endKey:        index[len(index)-1].Key,
	}
}

func (n *Node) GetAll() ([]*KVPair, error) {
	return n.sstReader.ReadData()
}

func (n *Node) Get(key []byte) ([]byte, bool, error) {
	index, ok := n.binarySearchIndex(key, 0, len(n.index)-1)
	if !ok {
		return nil, false, nil
	}

	bitmap := n.blockToFilter[index.PrevBlockOffset]
	if ok = n.conf.filter.Exist(bitmap, key); !ok {
		return nil, false, nil
	}

	block, err := n.sstReader.ReadBlock(index.PrevBlockOffset, index.PrevBlockSize)
	if err != nil {
		return nil, false, err
	}

	pairs, err := n.sstReader.ReadBlockData(block)
	if err != nil {
		return nil, false, err
	}

	for _, pair := range pairs {
		if bytes.Equal(pair.Key, key) {
			return pair.Value, true, nil
		}
	}

	return nil, false, nil
}

func (n *Node) binarySearchIndex(key []byte, start, end int) (*Index, bool) {
	if start == end {
		return n.index[start], bytes.Compare(n.index[start].Key, key) >= 0
	}

	mid := start + (end-start)>>1
	if bytes.Compare(n.index[mid].Key, key) < 0 {
		return n.binarySearchIndex(key, mid+1, end)
	}

	return n.binarySearchIndex(key, start, mid)
}

func (n *Node) Size() uint64 {
	return n.size
}
