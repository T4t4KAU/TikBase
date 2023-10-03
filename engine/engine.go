package engine

import (
	"TikBase/iface"
)

var engines = make(map[string]iface.Engine)

func RegisterEngine(name string, eng iface.Engine) {
	engines[name] = eng
}
