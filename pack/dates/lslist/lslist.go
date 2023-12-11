package lslist

import (
	"TikBase/engine/data"
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/dates/slist"
	"TikBase/pack/utils"
)

type List struct {
	*slist.List
}

func New() *List {
	return &List{
		slist.New(),
	}
}

func (list *List) Put(key []byte, pos *data.LogRecordPos) bool {
	b := data.EncodeLogRecordPos(pos)
	v := values.New(b, 0, iface.LOG_POS)
	return list.Insert(utils.B2S(key), &v)
}

func (list *List) Get(key []byte) *data.LogRecordPos {
	node, ok := list.Search(utils.B2S(key))
	if !ok {
		return &data.LogRecordPos{}
	}
	return data.DecodeLogRecordPos(node.Value.Bytes())
}

func (list *List) Delete(key []byte) bool {
	return list.Remove(utils.B2S(key))
}

func (list *List) Size() int {
	return 0
}

func (list *List) Iterator(reverse bool) iface.Iterator {
	return nil
}
