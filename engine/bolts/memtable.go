package bolts

import "github.com/T4t4KAU/TikBase/iface"

type MemTableCompactItem struct {
	walFile  string
	memTable iface.MemTable
}

func (t *Tree) compact() {
	for {
		select {}
	}
}
