package main

import (
	caches2 "TikCache/mode/caches"
	"TikCache/net"
	"flag"
	"log"
)

func main() {
	address := flag.String("address", "127.0.0.1:9960", "The address used to listen, such as 127.0.0.1:9960")

	options := caches2.DefaultOptions()
	flag.IntVar(&options.MaxEntrySize, "maxEntrySize", options.MaxEntrySize,
		"The max memory size that entries can use. The unit is GB.")
	flag.IntVar(&options.MaxGcCount, "maxGcCount", options.MaxGcCount,
		"The max count of entries that gc will clean.")
	flag.IntVar(&options.GcDuration, "gcDuration", options.GcDuration,
		"The duration between two gc tasks. The unit is Minute.")
	flag.StringVar(&options.DumpFile, "dumpFile", options.DumpFile,
		"The file used to dump the cache.")
	flag.IntVar(&options.DumpDuration, "dumpDuration", options.DumpDuration,
		"The duration between two dump tasks. The unit is Minute.")
	flag.IntVar(&options.MapSizeOfSegment, "mapSizeOfSegment", options.MapSizeOfSegment,
		"The map size of segment.")
	flag.IntVar(&options.SegmentSize, "segmentSize", options.SegmentSize,
		"The number of segment in a cache. This value should be the pow of 2 for precision.")
	flag.IntVar(&options.CasSleepTime, "casSleepTime", options.CasSleepTime,
		"The time of sleep in one cas step. The unit is Microsecond.")
	serverType := flag.String("serverType", "tcp", "The type of net (http, tcp).")

	flag.Parse()

	cache := caches2.NewCacheWith(options)
	cache.AutoDump()
	cache.AutoGC()
	log.Printf("cache-net is running on %s at %s", *serverType, *address)
	err := net.NewServer(*serverType, cache).Run(*address)
	if err != nil {
		panic(err)
	}
}
