package values

import (
	"TikBase/pack/dates/slist"
)

// ZSet 有序集合
type ZSet struct {
	dict  *Dict
	slist *slist.List // 跳表
}
