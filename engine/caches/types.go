package caches

import (
	"bytes"
	"encoding/gob"
	"github.com/T4t4KAU/TikBase/iface"
	"strings"
)

type Dict struct {
	m map[string]any
}

func NewDict() *Dict {
	return &Dict{
		m: make(map[string]any),
	}
}

func (dict *Dict) Get(key string) (any, bool) {
	val, ok := dict.m[key]
	return val, ok
}

func (dict *Dict) Len() int {
	if dict.m == nil {
		return 0
	}
	return len(dict.m)
}

func (dict *Dict) Put(key string, val any) bool {
	_, ok := dict.m[key]
	dict.m[key] = val
	if ok {
		return false
	}
	return true
}

func (dict *Dict) Remove(key string) (any, bool) {
	val, ok := dict.m[key]
	delete(dict.m, key)
	if ok {
		return val, false
	}
	return nil, true
}

func (dict *Dict) PutIfAbsent(key string, val iface.Value) bool {
	_, ok := dict.m[key]
	if ok {
		return false
	}
	dict.m[key] = val
	return true
}

func (dict *Dict) Keys() []string {
	keys := make([]string, len(dict.m))
	i := 0
	for key := range dict.m {
		keys[i] = key
		i++
	}
	return keys
}

func (dict *Dict) Clear() {
	*dict = *NewDict()
}

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

func (set *Set) Add(element string) bool {
	return set.dict.Put(element, nil)
}

func (set *Set) Remove(element string) bool {
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

type List struct {
	elements []string
}

func (list *List) LPush(element string) bool {
	slice := []string{element}
	slice = append(slice, list.elements...)
	list.elements = slice

	return true
}

func (list *List) RPush(element string) bool {
	list.elements = append(list.elements, element)
	return true
}

func (list *List) LPop() bool {
	if len(list.elements) < 1 {
		return false
	}
	list.elements = list.elements[1:]
	return true
}

func (list *List) RPop() bool {
	if len(list.elements) < 1 {
		return false
	}
	list.elements = list.elements[:len(list.elements)-1]
	return true
}
