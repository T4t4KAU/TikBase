package engine

import (
	"TikCache/engine/caches"
	"TikCache/engine/dates"
)

type Engine interface {
	Lookup(key string) (dates.Value, bool)
}

func NewEngine(name string) Engine {
	switch name {
	case "caches":
		return caches.NewCache()
	default:
		panic("invalid name")
	}
}
