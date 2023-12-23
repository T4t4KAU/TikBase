package types

import (
	"bytes"
	"encoding/gob"
	"github.com/T4t4KAU/TikBase/iface"
	"strings"
)

// Set 集合
type Set struct {
	dict *Dict
}

func (set *Set) Bytes() []byte {
	var buff bytes.Buffer

	encoder := gob.NewEncoder(&buff)
	_ = encoder.Encode(set.Elements())
	return buff.Bytes()
}

func (set *Set) Score() float32 {
	return 0
}

func (set *Set) Attr() iface.Type {
	return iface.SET
}

func (set *Set) Time() int64 {
	return 0
}

func NewSet() *Set {
	return &Set{
		dict: NewDict(),
	}
}

func (set *Set) Add(element string) int {
	return set.dict.Put(element, nil)
}

func (set *Set) Remove(element string) int {
	_, ret := set.dict.Remove(element)
	return ret
}

func (set *Set) Has(element string) bool {
	if set == nil || set.dict == nil {
		return false
	}
	_, ok := set.dict.Get(element)
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

func (set *Set) Elements() []string {
	return set.dict.Keys()
}
