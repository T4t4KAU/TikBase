package slist

import (
	"github.com/T4t4KAU/TikBase/engine/data"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"math/rand"
	"sync"
	"time"
)

// SkipList

// 是否向上延伸
func isInsertUp() bool {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(2)
	return n%2 == 0
}

// Node 跳表节点
type Node struct {
	Key   []byte
	Value *data.LogRecordPos
	Next  *Node // 指向后继结点
	Down  *Node // 指向下方结点
}

type Filter func(node *Node) bool

// Compare 比较结点大小
func (n *Node) Compare(node *Node) int {
	return utils.CompareKey(n.Key, node.Key)
}

func newNode(key []byte, value *data.LogRecordPos) *Node {
	return &Node{
		Key:   key,
		Value: value,
	}
}

type List struct {
	Head  *Node
	mutex sync.RWMutex
}

// New 创建跳表
func New() *List {
	return &List{
		// 创建头结点
		Head: newNode([]byte(""), nil),
	}
}

// Level 索引层数
func (list *List) Level() int {
	if list.Head == nil {
		return 0
	}

	var level int
	p := list.Head
	for p != nil {
		level++
		p = p.Down
	}
	return level - 1
}

// Insert 插入值
func (list *List) Insert(key []byte, pos *data.LogRecordPos) bool {
	if list == nil || list.Head == nil {
		return false
	}

	// 保存结点路径
	path := make([]*Node, 0)

	list.mutex.Lock()
	defer list.mutex.Unlock()

	p := list.Head

	// 从下往上逐层遍历
	// 找到插入值的前驱结点
	for p != nil {
		for p.Next != nil && utils.CompareBytes(p.Next.Key, key) < 0 {
			p = p.Next
		}
		// 将每层找到的结点存入路径
		path = append(path, p)
		p = p.Down
	}

	// 插入标识
	var insertUpFlag = true
	var downNode *Node

	// 向当前层增加结点
	for insertUpFlag && len(path) > 0 {
		node := newNode(key, pos)
		prevNode := path[len(path)-1]
		path = path[:len(path)-1]

		// 将新结点插入
		node.Next = prevNode.Next
		node.Down = downNode
		prevNode.Next = node
		downNode = node

		// 随机选择是否向上建立索引
		insertUpFlag = isInsertUp()
	}

	// 建立新的层
	if len(path) <= 0 && isInsertUp() {
		node := newNode(key, pos)
		node.Down = downNode
		newHead := newNode([]byte(""), nil)
		newHead.Next = node
		newHead.Down = list.Head
		list.Head = newHead
	}

	return true
}

// Remove 删除元素
func (list *List) Remove(key []byte) bool {
	p, ok := list.Head, false

	list.mutex.Lock()
	defer list.mutex.Unlock()

	for p != nil {
		// 该层链表未到达末尾前 找到不大于key的最大结点
		for p.Next != nil && utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Next
		}

		// 该层链表到末尾或者到达最大key则下降
		if p.Next == nil || utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Down
		} else {
			p.Next = p.Next.Next
			p = p.Down
			ok = true
		}
	}
	return ok
}

// Search 搜索
func (list *List) Search(key []byte) (*Node, bool) {
	p := list.Head

	list.mutex.RLock()
	defer list.mutex.RUnlock()

	for p != nil {
		for p.Next != nil && utils.CompareBytes(p.Next.Key, key) < 0 {
			p = p.Next
		}

		// 在该层搜索不到 下降到下一层
		if p.Next == nil || utils.CompareBytes(p.Next.Key, key) < 0 {
			p = p.Down
		} else {
			return p.Next, true
		}
	}
	return &Node{}, false
}

func (list *List) Update(key []byte, pos *data.LogRecordPos) bool {
	p, ok := list.Head, false

	list.mutex.Lock()
	defer list.mutex.Unlock()

	for p != nil {
		// 该层链表未到达末尾前 找到不大于key的最大结点
		for p.Next != nil && utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Next
		}

		// 该层链表到末尾或者到达最大key则下降
		if p.Next == nil || utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Down
		} else {
			p.Next.Value = pos
			p = p.Down
			ok = true
		}
	}
	return ok
}

type Iterator struct {
}

func (it *Iterator) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Key() []byte {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Value() *data.LogRecordPos {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Close() {
	//TODO implement me
	panic("implement me")
}
