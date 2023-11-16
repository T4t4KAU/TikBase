package engine

import (
	"TikBase/iface"
	"errors"
)

var engines = make(map[string]iface.Engine)

func RegisterEngine(name string, eng iface.Engine) {
	engines[name] = eng
}

func NewEngine(name string) (iface.Engine, error) {
	switch name {
	case "cache":
		return NewCacheEngine()
	case "level":
		return NewLevelEngine()
	case "base":
		return NewBaseEngine()
	default:
		return nil, errors.New("invalid engine")
	}
}
