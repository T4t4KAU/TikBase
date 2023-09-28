package caches

// Status 缓存信息
type Status struct {
	Count     int   // 记录缓存数据个数
	KeySize   int64 // 记录key占用空间大小
	ValueSize int64 // 记录value占用空间大小
}

// NewStatus 返回一个缓存信息对象指针
func NewStatus() *Status {
	return &Status{
		Count:     0,
		KeySize:   0,
		ValueSize: 0,
	}
}

// 储存键值对后更新状态信息
func (s *Status) addEntry(key string, value []byte) {
	s.Count++
	s.KeySize += int64(len(key))
	s.ValueSize += int64(len(value))
}

// 删除键值对后更新状态信息
func (s *Status) subEntry(key string, value []byte) {
	s.Count--
	s.KeySize -= int64(len(key))
	s.ValueSize -= int64(len(value))
}

// 返回键值对占用总和
func (s *Status) entrySize() int64 {
	return s.KeySize + s.ValueSize
}
