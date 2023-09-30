package levels

import (
	"TikBase/pack/dates/slist"
	"TikBase/pack/iface"
	"sync"
)

type Levels struct {
	mutex sync.RWMutex
	*slist.List
}

func New() *Levels {
	return &Levels{
		List: slist.New(),
	}
}

func (ls *Levels) Get(key string) (iface.Value, bool) {
	ls.mutex.RLock()
	defer ls.mutex.RUnlock()

	node, ok := ls.Search(key)
	return node.Value, ok
}

func (ls *Levels) Set(key string, value iface.Value) bool {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	_, ok := ls.Search(key)
	if !ok {
		return ls.Insert(key, value)
	}
	return ls.Update(key, value)
}

func (ls *Levels) Del(key string) bool {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	return ls.Remove(key)
}

func (ls *Levels) Exist(key string) bool {
	ls.mutex.RLock()
	defer ls.mutex.RUnlock()

	_, ok := ls.Search(key)
	return ok
}

func (ls *Levels) gc() {
	keys := ls.FilterKey(func(node *slist.Node) bool {
		return !node.Value.Alive()
	})

	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	for _, key := range *keys {
		ls.Remove(key)
	}
}
