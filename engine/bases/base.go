package bases

import (
	"TikBase/iface"
)

type Base struct {
}

func (b *Base) Get(key string) (iface.Value, bool) {
	//TODO implement me
	panic("implement me")
}

func (b *Base) Set(key string, value iface.Value) bool {
	//TODO implement me
	panic("implement me")
}

func (b *Base) Del(key string) bool {
	//TODO implement me
	panic("implement me")
}
