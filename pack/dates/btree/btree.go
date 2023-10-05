package btree

// B Tree

type Node struct {
	key      string
	children []*Node
	parent   *Node
	size     int
}

type Tree struct {
	Root *Node
}

func New() *Tree {
	return &Tree{
		Root: nil,
	}
}
