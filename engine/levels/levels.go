package levels

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/dates/slist"
)

type Levels struct {
	*slist.List
}

func New() (*Levels, error) {
	return &Levels{
		List: slist.New(),
	}, nil
}

func (ls *Levels) Get(key string) (iface.Value, bool) {
	node, ok := ls.Search(key)

	if !ok {
		return nil, false
	}

	if !node.Value.Alive() {
		ls.Remove(node.Key)
		return nil, false
	}
	return node.Value, ok
}

func (ls *Levels) Set(key string, value iface.Value) bool {
	_, ok := ls.Search(key)
	if !ok {
		return ls.Insert(key, value)
	}
	return ls.Update(key, value)
}

func (ls *Levels) Del(key string) bool {
	return ls.Remove(key)
}

func (ls *Levels) Exist(key string) bool {
	_, ok := ls.Search(key)
	return ok
}

func (ls *Levels) Expire(key string, ttl int64) bool {
	node, ok := ls.Search(key)
	if !ok {
		return false
	}
	node.Value.(*values.Value).TTL = ttl
	return true
}

func (ls *Levels) gc() {
	keys := ls.FilterKey(func(node *slist.Node) bool {
		return !node.Value.Alive()
	})

	for _, key := range *keys {
		ls.Remove(key)
	}
}
