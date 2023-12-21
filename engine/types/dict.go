package types

import "TikBase/iface"

// Dict 字典
type Dict struct {
	m map[iface.Value]any
}

func NewDict() *Dict {
	return &Dict{
		m: make(map[iface.Value]any),
	}
}

func (dict *Dict) Get(key iface.Value) (any, bool) {
	val, ok := dict.m[key]
	return val, ok
}

func (dict *Dict) Len() int {
	if dict.m == nil {
		return 0
	}
	return len(dict.m)
}

func (dict *Dict) Put(key iface.Value, val any) int {
	_, ok := dict.m[key]
	dict.m[key] = val
	if ok {
		return 0
	}
	return 1
}

func (dict *Dict) Remove(key iface.Value) (any, int) {
	val, ok := dict.m[key]
	delete(dict.m, key)
	if ok {
		return val, 1
	}
	return nil, 0
}

func (dict *Dict) PutIfAbsent(key iface.Value, val iface.Value) int {
	_, ok := dict.m[key]
	if ok {
		return 0
	}
	dict.m[key] = val
	return 1
}

func (dict *Dict) Keys() []iface.Value {
	keys := make([]iface.Value, len(dict.m))
	i := 0
	for k := range dict.m {
		keys[i] = k
		i++
	}
	return keys
}

func (dict *Dict) Clear() {
	*dict = *NewDict()
}
