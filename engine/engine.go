package engine

import "TikBase/pack/iface"

var engines = make(map[string]iface.Engine)

func RegisterEngine(name string, eng iface.Engine) {
	engines[name] = eng
}
