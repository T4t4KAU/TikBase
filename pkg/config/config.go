package config

import (
	"fmt"
	"github.com/T4t4KAU/TikBase/engine/bases"
	"github.com/spf13/viper"
)

type StoreConfig interface{}

type BaseStoreConfig struct {
	Directory    string `mapstructure:"directory"`
	Indexer      string `mapstructure:"indexer"`
	DatafileSize int    `mapstructure:"datafile_size"`
	BytesPerSync int    `mapstructure:"bytes_per_sync"`
	SyncWrites   bool   `mapstructure:"sync_writes"`
	WriteBatch   struct {
		MaxBatchNum int  `mapstructure:"max_batch_num"`
		SyncWrites  bool `mapstructure:"sync_writes"`
	} `mapstructure:"write_batch"`
	DatafileMergeRatio int `mapstructure:"datafile_merge_ratio"`
	Iterator           struct {
		Prefix  string `mapstructure:"prefix"`
		Reverse bool   `mapstructure:"reverse"`
	} `mapstructure:"iterator"`
	MmapAtStartup bool `mapstructure:"mmap_at_startup"`
}

type CacheStoreConfig struct {
	MaxEntrySize     uint   `mapstructure:"max_entry_size"`
	MaxGcCount       uint   `mapstructure:"max_gc_count"`
	GcDuration       uint   `mapstructure:"gc_duration"`
	DumpFile         string `mapstructure:"dump_file"`
	DumpDuration     uint   `mapstructure:"dump_duration"`
	MapSizeOfSegment uint   `mapstructure:"map_size_of_segment"`
	SegmentSize      uint   `mapstructure:"segment_size"`
	CasSleepTime     uint   `mapstructure:"cas_sleep_time"`
}

type ServerConfig struct {
	ListenPort int    `mapstructure:"listen_port"`
	HTTPPort   int    `mapstructure:"http_port"`
	WorkersNum int    `mapstructure:"workers_num"`
	Timeout    int    `mapstructure:"timeout"`
	EngineName string `mapstructure:"engine"`
	Protocol   string `mapstructure:"protocol"`
}

func ReadServerConfigFile(filePath string) (ServerConfig, error) {
	viper.SetConfigFile(filePath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return ServerConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var config ServerConfig
	err = viper.Unmarshal(&config)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

func ReadBaseConfigFile(filePath string) (StoreConfig, error) {
	viper.SetConfigFile(filePath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return BaseStoreConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}
	var config BaseStoreConfig
	err = viper.Unmarshal(&config)
	if err != nil {
		return BaseStoreConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

func ReadCacheConfigFile(filepath string) (StoreConfig, error) {
	viper.SetConfigFile(filepath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return CacheStoreConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var config CacheStoreConfig
	err = viper.Unmarshal(&config)
	if err != nil {
		return CacheStoreConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

func BaseEngineConfig(config BaseStoreConfig) (bases.Options, bases.WriteBatchOptions, bases.IteratorOptions) {
	var idx bases.IndexerType
	switch config.Indexer {
	case "art", "":
		idx = bases.ART
	case "bt":
		idx = bases.BT
	default:
		panic("invalid index type")
	}

	if config.BytesPerSync <= 0 {
		panic("invalid bytes_per_sync param")
	}
	if config.DatafileSize <= 0 {
		panic("invalid data_file_size param")
	}
	if config.DatafileMergeRatio <= 0 {
		panic("invalid datafile_merge_ratio param")
	}

	baseOption := bases.Options{
		DirPath:            config.Directory,
		DataFileSize:       int64(config.DatafileSize),
		SyncWrites:         config.SyncWrites,
		IndexType:          idx,
		BytesPerSync:       uint(config.BytesPerSync),
		MMapAtStartup:      config.MmapAtStartup,
		DataFileMergeRatio: float32(config.DatafileMergeRatio),
	}

	iterOption := bases.IteratorOptions{
		Prefix:  []byte(config.Iterator.Prefix),
		Reverse: config.Iterator.Reverse,
	}

	if config.WriteBatch.MaxBatchNum <= 0 {
		panic("invalid max_batch_num param")
	}

	txOption := bases.WriteBatchOptions{
		MaxBatchNum: uint(config.WriteBatch.MaxBatchNum),
		SyncWriters: config.WriteBatch.SyncWrites,
	}

	return baseOption, txOption, iterOption
}

type ReplicaConfig struct {
	Id            string `mapstructure:"node_id"`
	Count         int    `mapstructure:"replicas"`
	Address       string `mapstructure:"bind_addr"`
	DirPath       string `mapstructure:"dir_path"`
	WorkerNum     int    `mapstructure:"worker_num"`
	SnapshotCount int    `mapstructure:"snapshot_count"`
	Timeout       int    `mapstructure:"timeout"`
	JoinAddr      string `mapstructure:"join_addr"`
	ServiceAddr   string `mapstructure:"service_addr"`
	TargetAddr    string `mapstructure:"target_addr"`
}

func ReadReplicaConfigFile(filePath string) (ReplicaConfig, error) {
	viper.SetConfigFile(filePath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return ReplicaConfig{}, err
	}

	var config ReplicaConfig
	err = viper.Unmarshal(&config)
	if err != nil {
		return ReplicaConfig{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}
