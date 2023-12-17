package engine

import (
	"TikBase/iface"
	"TikBase/pack/errno"
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

// 解析参数

func parseSetStringArgs(args [][]byte) (string, error) {
	if len(args) < 2 {
		return "", errno.ErrParseArgsError
	}
	return string(args[1]), nil
}

func parseExpireKeyArgs(args [][]byte) (int64, error) {
	if len(args) < 2 {
		return 0, errno.ErrParseArgsError
	}
	return utils.B2I64(args[1]), nil
}

func parseHashSetArgs(args [][]byte) ([]byte, []byte, []byte, error) {
	if len(args) < 3 {
		return nil, nil, nil, errno.ErrParseArgsError
	}
	return args[0], args[1], args[2], nil
}

func parseHashGetArgs(args [][]byte) ([]byte, []byte, error) {
	if len(args) < 2 {
		return nil, nil, errno.ErrParseArgsError
	}
	return args[0], args[1], nil
}

func parseHashDelArgs(args [][]byte) ([]byte, []byte, error) {
	if len(args) < 2 {
		return nil, nil, errno.ErrParseArgsError
	}
	return args[0], args[1], nil
}

func parseSetAddArgs(args [][]byte) ([]byte, []byte, error) {
	if len(args) < 3 {
		return nil, nil, errno.ErrParseArgsError
	}
	return args[0], args[1], nil
}
