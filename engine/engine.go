package engine

import (
	"TikCache/engine/caches"
)

type Engine interface {
}

type Value interface {
}

type KVStore interface {
	Lookup(key string) (Value, bool)
}

func NewEngine(name string) Engine {
	switch name {
	case "cache":
		return caches.NewCache()
	default:
		return caches.NewCache()
	}
}
