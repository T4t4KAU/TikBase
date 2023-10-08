package slist

import (
	"TikBase/iface"
	"TikBase/pack/utils"
	"fmt"
	"math/rand"
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
	Key   string
	Value iface.Value
	Next  *Node // 指向后继结点
	Down  *Node // 指向下方结点
}

type Filter func(node *Node) bool

// Compare 比较结点大小
func (n *Node) Compare(node *Node) int {
	return utils.CompareKey(n.Key, node.Key)
}

func newNode(key string, value iface.Value) *Node {
	return &Node{
		Key:   key,
		Value: value,
	}
}

type List struct {
	Head *Node
}

// New 创建跳表
func New() *List {
	return &List{
		// 创建头结点
		Head: newNode("", nil),
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
func (list *List) Insert(key string, val iface.Value) bool {
	if list == nil || list.Head == nil {
		return false
	}

	// 保存结点路径
	path := make([]*Node, 0)
	p := list.Head

	// 从下往上逐层遍历
	// 找到插入值的前驱结点
	for p != nil {
		for p.Next != nil && utils.CompareKey(p.Next.Key, key) < 0 {
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
		node := newNode(key, val)
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
		node := newNode(key, val)
		node.Down = downNode
		newHead := newNode("", nil)
		newHead.Next = node
		newHead.Down = list.Head
		list.Head = newHead
	}

	return true
}

// Print 打印跳表
func (list *List) Print() {
	if list.Head == nil || list.Head.Next == nil {
		return
	}

	// 从顶层开始打印
	level := list.Level()
	curr := list.Head

	for level >= 0 {
		node := curr.Next

		fmt.Printf("Level %d: ", level)
		for node != nil {
			fmt.Printf("%v -> ", node.Value.String())
			node = node.Next
		}
		fmt.Println()

		curr = curr.Down
		level--
	}
}

// Remove 删除元素
func (list *List) Remove(key string) bool {
	p, ok := list.Head, false
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
func (list *List) Search(key string) (*Node, bool) {
	p := list.Head
	for p != nil {
		for p.Next != nil && utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Next
		}

		// 在该层搜索不到 下降到下一层
		if p.Next == nil || utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Down
		} else {
			return p.Next, true
		}
	}
	return &Node{}, false
}

func (list *List) Update(key string, val iface.Value) bool {
	p, ok := list.Head, false
	for p != nil {
		// 该层链表未到达末尾前 找到不大于key的最大结点
		for p.Next != nil && utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Next
		}

		// 该层链表到末尾或者到达最大key则下降
		if p.Next == nil || utils.CompareKey(p.Next.Key, key) < 0 {
			p = p.Down
		} else {
			p.Next.Value = val
			p = p.Down
			ok = true
		}
	}
	return ok
}

func (list *List) FilterKey(f Filter) *[]string {
	p := list.Head
	keys := make([]string, 0)

	for p.Down != nil {
		p = p.Down
	}

	for p.Next != nil {
		if f(p.Next) {
			keys = append(keys, p.Next.Key)
		}
		p = p.Next
	}
	return &keys
}

func (list *List) FilterNode(filter Filter) *[]*Node {
	p := list.Head
	nodes := make([]*Node, 0)

	for p.Down != nil {
		p = p.Down
	}

	for p.Next != nil {
		if filter(p.Next) {
			nodes = append(nodes, p.Next)
		}
		p = p.Next
	}
	return &nodes
}
