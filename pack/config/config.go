package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type StoreConfig struct {
	Type         string `mapstructure:"type"`
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
	TCPPort    int `mapstructure:"tcp_port"`
	HTTPPort   int `mapstructure:"http_port"`
	WorkersNum int `mapstructure:"workers_num"`
	Timeout    int `mapstructure:"timeout"`
}

type Config struct {
	Store  StoreConfig  `mapstructure:"store"`
	Server ServerConfig `mapstructure:"server"`
}

func ReadConfigFile(filePath string) (Config, error) {
	viper.SetConfigFile(filePath)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return config, nil
}
