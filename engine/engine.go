package engine

import (
	"TikCache/engine/caches"
)

type Engine interface {
	Lookup(key string) (Value, bool)
}

type Value interface {
	Compare(Value) int
}

func NewEngine(name string) Engine {
	switch name {
	case "caches":
		return caches.NewCache()
	default:
		return caches.NewCache()
	}
}
