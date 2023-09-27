package slist

import (
	"TikCache/pack/dates"
	"fmt"
	"math/rand"
	"time"
)

// SkipList

func isInsertUp() bool {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(2)
	return n%2 == 0
}

// Node 跳表节点
type Node[T dates.Sortable] struct {
	Value T
	Next  *Node[T] // 指向后继结点
	Down  *Node[T] // 指向下方结点
}

func newNode[T dates.Sortable](value T) *Node[T] {
	return &Node[T]{
		Value: value,
	}
}

type List[T dates.Sortable] struct {
	Head *Node[T]
}

// New 创建跳表
func New[T dates.Sortable]() *List[T] {
	return &List[T]{
		Head: newNode[T](-1),
	}
}

// Insert 插入值
func (list *List[T]) Insert(value T) {
	// 保存结点路径
	path := make([]*Node[T], 0)
	p := list.Head

	// 从下往上逐层遍历
	// 找到插入值的前驱结点
	for p != nil {
		for p.Next != nil && p.Next.Value < value {
			p = p.Next
		}
		// 将每层找到的结点存入
		path = append(path, p)
		p = p.Down
	}

	// 插入标识
	var insertUpFlag = true
	var downNode *Node[T]

	// 向当前层增加结点
	for insertUpFlag && len(path) > 0 {
		node := newNode(value)
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
		node := newNode[T](value)
		node.Down = downNode
		newHead := newNode[T](-1)
		newHead.Next = node
		newHead.Down = list.Head
		list.Head = newHead
	}
}

// Level 索引层数
func (list *List[T]) Level() int {
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

// Print 打印跳表
func (list *List[T]) Print() {
	if list.Head == nil || list.Head.Next == nil {
		fmt.Println("跳表为空")
		return
	}

	// 从顶层开始打印
	level := list.Level()
	curr := list.Head

	for level >= 0 {
		node := curr.Next

		fmt.Printf("Level %d: ", level)
		for node != nil {
			fmt.Printf("%v -> ", node.Value)
			node = node.Next
		}
		fmt.Println()

		curr = curr.Down
		level--
	}
}

// Remove 删除元素
func (list *List[T]) Remove(value T) bool {
	p, ok := list.Head, false
	for p != nil {
		for p.Next != nil && p.Next.Value < value {
			p = p.Next
		}
		if p.Next == nil || p.Next.Value > value {
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
func (list *List[T]) Search(value T) (*Node[T], bool) {
	p := list.Head
	for p != nil {
		for p.Next != nil && p.Next.Value < value {
			p = p.Next
		}

		// 在该层搜索不到 下降到下一层
		if p.Next == nil || p.Next.Value > value {
			p = p.Down
		} else {
			return p.Next, true
		}
	}
	return &Node[T]{}, false
}
