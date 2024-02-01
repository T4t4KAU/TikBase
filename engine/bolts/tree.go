package bolts

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/engine/wal"
	"github.com/T4t4KAU/TikBase/iface"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Tree struct {
	conf *Config

	options    Options
	dataLock   sync.RWMutex   // 读写数据时使用的锁
	levelLocks []sync.RWMutex // 每层节点使用的读写锁

	memTable       iface.MemTable         // 读写memtable
	rdOnlyMemTable []*MemTableCompactItem // 只读memtable
	walWriter      wal.Writer             // 预写日志写入口
	nodes          [][]*Node              // 树状数据结构

	memCompactCh   chan *MemTableCompactItem //
	levelCompactCh chan int
	stopCh         chan struct{}

	memTableIndex int
	levelToSeq    []atomic.Int32
}

func (t *Tree) sstFile(level int, seq int32) string {
	return fmt.Sprintf("%d_%d.sst", level, seq)
}

func (t *Tree) walFile() string {
	return path.Join(t.conf.DirPath, "walfile", fmt.Sprintf("%d.wal", t.memTableIndex))
}

func (t *Tree) flushMemTable(memTable iface.MemTable) {
	seq := t.levelToSeq[0].Load() + 1
	sstWriter, _ := NewSSTWriter(t.sstFile(0, seq), t.conf)

}

func walFileToMemTableIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, ".wal", "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}
