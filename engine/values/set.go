package values

import "strings"

// Set 集合
type Set struct {
	dict *Dict
}

func NewSet() *Set {
	return &Set{
		dict: NewDict(),
	}
}

func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

func (set *Set) Remove(val string) int {
	_, ret := set.dict.Remove(val)
	return ret
}

func (set *Set) Has(val string) bool {
	if set == nil || set.dict == nil {
		return false
	}
	_, ok := set.dict.Get(val)
	return ok
}

func (set *Set) Len() int {
	if set == nil || set.dict == nil {
		return 0
	}
	return set.dict.Len()
}

func (set *Set) String() string {
	s := ""
	for k := range set.dict.m {
		s += k + ", "
	}

	s = strings.TrimRight(s, ", ")
	return "{" + s + "}"
}
