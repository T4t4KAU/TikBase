package levels

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/dates/slist"
	"fmt"
	"testing"
)

func TestLevels_Get(t *testing.T) {
	c := New()
	v := values.New([]byte("value1"), 0, iface.STRING)
	c.Set("key1", &v)
	res, _ := c.Get("key1")
	println(res.String())
	v = values.New([]byte("value2"), 0, iface.STRING)
	c.Set("key2", &v)
	res, _ = c.Get("key2")
	println(res.String())

	nodes := c.FilterNode(func(node *slist.Node) bool {
		return true
	})

	for _, node := range *nodes {
		fmt.Printf("%s : %s\n", node.Key, node.Value)
	}
}
