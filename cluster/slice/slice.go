package slice

import "github.com/T4t4KAU/TikBase/cluster/chash"

// Slice 数据切片
type Slice struct {
	chash.ConsistentHash
}

func New() (*Slice, error) {
	return &Slice{}, nil
}
