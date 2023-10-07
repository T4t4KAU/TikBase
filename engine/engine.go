package engine

import (
	"TikBase/iface"
	"errors"
)

var engines = make(map[string]iface.Engine)

var (
	errKeyNotFound    = errors.New("key not found")
	errExceedCapacity = errors.New("data exceeds capacity")
)

func RegisterEngine(name string, eng iface.Engine) {
	engines[name] = eng
}

func NewEngine(name string) (iface.Engine, error) {
	switch name {
	case "cache":
		return NewCacheEngine(), nil
	case "level":
		return NewLevelEngine(), nil
	default:
		return nil, errors.New("invalid engine")
	}
}
