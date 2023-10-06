package chash

import (
	"sort"
	"strconv"
)

type HashFunc func(data []byte) uint32

// ConsistentHash 一致性哈希算法结构体
type ConsistentHash struct {
	Nodes       []uint32          // 节点的哈希值列表
	Replicas    int               // 虚拟节点的复制因子
	Hash        HashFunc          // 哈希函数
	KeysMapping map[uint32]string // 节点的哈希值与节点名称的映射
}

var DefaultHash = func(key []byte) uint32 {
	i, _ := strconv.Atoi(string(key))
	return uint32(i)
}

func New(replicas int, hash HashFunc) *ConsistentHash {
	return &ConsistentHash{
		Replicas:    replicas,
		Hash:        hash,
		KeysMapping: make(map[uint32]string),
	}
}

// AddNode 添加节点
func (c *ConsistentHash) AddNode(nodes ...string) {
	for _, node := range nodes {
		for i := 0; i < c.Replicas; i++ {
			hash := c.Hash([]byte(strconv.Itoa(i) + node))
			c.Nodes = append(c.Nodes, hash)
			c.KeysMapping[hash] = node
		}
	}
	sort.Slice(c.Nodes, func(i, j int) bool {
		return c.Nodes[i] < c.Nodes[j]
	})
}

// RemoveNode 移除节点
func (c *ConsistentHash) RemoveNode(node string) {
	for i := 0; i < c.Replicas; i++ {
		hash := c.Hash([]byte(strconv.Itoa(i) + node))
		index := -1
		for j, nodeHash := range c.Nodes {
			if nodeHash == hash {
				index = j
				break
			}
		}
		if index != -1 {
			c.Nodes = append(c.Nodes[:index], c.Nodes[index+1:]...)
			delete(c.KeysMapping, hash)
		}
	}
}

// GetNode 根据 key 获取对应的节点
func (c *ConsistentHash) GetNode(key string) string {
	if len(c.Nodes) == 0 {
		return ""
	}
	hash := c.Hash([]byte(key))
	idx := sort.Search(len(c.Nodes), func(i int) bool {
		return c.Nodes[i] >= hash
	})
	if idx == len(c.Nodes) {
		idx = 0
	}
	return c.KeysMapping[c.Nodes[idx]]
}
