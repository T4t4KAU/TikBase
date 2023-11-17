package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	Name     string `json:"Name"`
	Host     string `json:"Host"`
	TcpPort  int    `json:"TcpPort"`
	MaxConn  int    `json:"MaxConn"`
	Protocol string `json:"Protocol"`
	Type     string `json:"Type"`
}

func ReadConfigFile(filename string) (*Config, error) {
	viper.SetConfigFile(filename)

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("read config file failed: %w", err)
	}

	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("parse config file failed: %w", err)
	}

	return &config, nil
}
