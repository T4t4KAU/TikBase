package caches

type Options struct {
	MaxEntrySize     int    // 写满保护阈值 当缓存中键值对占用空间达到阈值 出发写满保护
	MaxGcCount       int    // 自动淘汰阈值 当清理的数据达到该值就会停止清理
	GcDuration       int    // 淘汰之间间隔(min) 每隔固定时间进行一次自动淘汰
	DumpFile         string // 持久化路径
	DumpDuration     int    // 持久化时间间隔
	MapSizeOfSegment int    // segment map初始化大小
	SegmentSize      int    // 缓存中有多少个segment
	CasSleepTime     int    // CAS自旋等待时间
}

// DefaultOptions 返回默认的选项配置
func DefaultOptions() Options {
	return Options{
		MaxEntrySize:     4,
		MaxGcCount:       10,
		GcDuration:       60,
		DumpFile:         "cache.dump",
		DumpDuration:     30,
		MapSizeOfSegment: 256,
		SegmentSize:      1024,
		CasSleepTime:     1000,
	}
}
