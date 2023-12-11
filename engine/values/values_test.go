package values

import (
	"TikBase/engine/types"
	"testing"
)

func TestSet_Add(t *testing.T) {
	s := types.NewSet()
	s.Add("a")
	s.Add("a")
	s.Add("b")

	println(s.String())
}

func TestSet_Remove(t *testing.T) {
	s := types.NewSet()
	s.Add("a")
	s.Add("a")
	s.Add("b")
	s.Remove("b")

	println(s.String())
}

func TestSet_Len(t *testing.T) {
	s := types.NewSet()
	s.Add("a")
	s.Add("b")

	println(s.Len())
}
