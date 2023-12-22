package types

import (
	"TikBase/pack/dates/slist"
)

// ZSet 有序集合
type ZSet struct {
	dict  *Dict
	slist *slist.List // 跳表
}

func (set *ZSet) Add(element string) {
	set.dict.Put(element, nil)
	set.slist.Insert(element, nil)
}

func (set *ZSet) Remove(element string) {
	set.dict.Remove(element)
	set.slist.Remove(element)
}

func (set *ZSet) Contain(element string) bool {
	_, ok := set.dict.Get(element)
	return ok
}

func (set *ZSet) Bytes() []byte {
	var s string
	keys := set.dict.Keys()

	for _, key := range keys {
		s += key + "\r\n"
	}
	return []byte(s)
}

func (set *ZSet) String() string {
	return ""
}
