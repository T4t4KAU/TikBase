package types

import "github.com/T4t4KAU/TikBase/iface"

// Dict 字典
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

func (dict *Dict) Put(key string, val any) int {
	_, ok := dict.m[key]
	dict.m[key] = val
	if ok {
		return 0
	}
	return 1
}

func (dict *Dict) Remove(key string) (any, int) {
	val, ok := dict.m[key]
	delete(dict.m, key)
	if ok {
		return val, 1
	}
	return nil, 0
}

func (dict *Dict) PutIfAbsent(key string, val iface.Value) int {
	_, ok := dict.m[key]
	if ok {
		return 0
	}
	dict.m[key] = val
	return 1
}

func (dict *Dict) Keys() []string {
	keys := make([]string, len(dict.m))
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
