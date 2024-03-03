package engine

import (
	"errors"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/errno"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

type ExecFunc func(args [][]byte) iface.Result

var engines = make(map[string]iface.Engine)

// RegisterEngine 注册存储引擎
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

//
// 对参数进行解析
//

func ParseStrGetArgs(args [][]byte) (string, error) {
	if len(args) < 1 {
		return "", errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), nil
}

func MakeStrGetArgs(key string) [][]byte {
	return [][]byte{
		utils.S2B(key),
	}
}

func ParseStrSetArgs(args [][]byte) (string, string, error) {
	if len(args) < 2 {
		return "", "", errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), utils.B2S(args[1]), nil
}

func MakeStrSetArgs(key string, value []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		value,
	}
}

func ParseDelKeyArgs(args [][]byte) (string, error) {
	if len(args) < 1 {
		return "", errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), nil
}

func MakeDelKeyArgs(key string) [][]byte {
	return [][]byte{
		utils.S2B(key),
	}
}

func ParseExpireKeyArgs(args [][]byte) (string, int64, error) {
	if len(args) < 2 {
		return "", 0, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), utils.B2I64(args[1]), nil
}

func MakeExpireKeyArgs(key string, ttl int64) [][]byte {
	return [][]byte{
		utils.S2B(key),
		utils.I642B(ttl),
	}
}

func ParseHashSetArgs(args [][]byte) (string, []byte, []byte, error) {
	if len(args) < 3 {
		return "", nil, nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], args[2], nil
}

func MakeHashSetArgs(key string, field, value []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		field,
		value,
	}
}

func ParseHashGetArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func MakeHashGetArgs(key string, field []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		field,
	}
}

func ParseListPushArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return utils.B2S(args[0]), args[1], errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func MakeListPushArgs(key string, element []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		element,
	}
}

func ParseListPopArgs(args [][]byte) (string, error) {
	if len(args) < 1 {
		return utils.B2S(args[0]), errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), nil
}

func MakeListPopArgs(key string) [][]byte {
	return [][]byte{
		utils.S2B(key),
	}
}

func ParseHashDelArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func MakeHashDelArgs(key string, field []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		field,
	}
}

func ParseSetAddArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func MakeSetAddArgs(key string, element []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		element,
	}
}

func ParseSetRemArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func MakeSetRemArgs(key string, element []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		element,
	}
}

func ParseSetIsMemberArgs(args [][]byte) (string, []byte, error) {
	if len(args) < 2 {
		return "", nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), args[1], nil
}

func MakeSetIsMemberArgs(key string, element []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		element,
	}
}

func ParseZSetAddArgs(args [][]byte) (string, float64, []byte, error) {
	if len(args) < 3 {
		return "", 0, nil, errno.ErrParseArgsError
	}
	return utils.B2S(args[0]), utils.B2F64(args[1]), args[2], nil
}

func MakeZSetAddArgs(key string, score float64, element []byte) [][]byte {
	return [][]byte{
		utils.S2B(key),
		utils.F642B(score),
		element,
	}
}
