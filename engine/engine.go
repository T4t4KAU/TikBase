package engine

import (
	"TikBase/iface"
	"TikBase/pack/utils"
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

func parseSetStringArgs(args [][]byte) string {
	return string(args[1])
}

func parseExpireKeyArgs(args [][]byte) int64 {
	return utils.BytesToInt64(args[1])
}
