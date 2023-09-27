package tcp

import (
	"errors"
	"strings"
)

const (
	getCommand    = byte(1)
	setCommand    = byte(2)
	deleteCommand = byte(3)
	statusCommand = byte(4)
)

var (
	errCommandNeedsMoreArguments = errors.New("command needs more arguments")
	errNotFound                  = errors.New("not found")
	errProtocolVersionMismatch   = errors.New("protocol version between client and proto doesn't match")
	errCommandHandlerNotFound    = errors.New("failed to find a handler of command")
)

const (
	ProtocolVersion        = byte(1)
	headerLengthInProtocol = 6
	argsLengthInProtocol   = 4
	argLengthInProtocol    = 4
	bodyLengthInProtocol   = 4
)

func checkNetworkError(err error) bool {
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	return false
}
