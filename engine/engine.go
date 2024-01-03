package engine

import (
	"errors"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

type ExecFunc func(args [][]byte) iface.Result

var engines = make(map[string]iface.Engine)

func RegisterEngine(name string, eng iface.Engine) {
	engines[name] = eng
}

func NewEngine(name string) (iface.Engine, error) {
	switch name {
	case "cache":
		return NewCacheEngine()
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

func parseHashSetArgs(args [][]byte) (string, []byte, []byte, error) {
	if len(args) < 3 {
		return "", nil, nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], args[2], nil
}

func parseHashGetArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func parseListPushArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return utils.B2S(args[0]), args[1], errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func parseListPopArgs(args [][]byte) (string, error) {
	if len(args) < 1 {
		return utils.B2S(args[0]), errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), nil
}

func parseHashDelArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func parseSetAddArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func parseSetRemArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func parseSetIsMemberArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func parseZSetAddArgs(args [][]byte) (string, float64, []byte, error) {
	if len(args) < 3 {
		return "", 0, nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), utils.B2F64(args[1]), args[2], nil
}
