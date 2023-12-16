package config

import (
	"TikBase/engine/bases"
	"fmt"

	"github.com/spf13/viper"
)

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

type ServerConfig struct {
	RESPPort   int `mapstructure:"resp_port"`
	HTTPPort   int `mapstructure:"http_port"`
	WorkersNum int `mapstructure:"workers_num"`
	Timeout    int `mapstructure:"timeout"`
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

func ReadBaseConfigFile(filePath string) (BaseStoreConfig, error) {
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
