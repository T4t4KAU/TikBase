package config

import (
	"fmt"
	"testing"
)

func TestReadServerConfigFile(t *testing.T) {
	c, err := ReadServerConfigFile("../../config/server-config.yaml")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%#v\n", c)
}

func TestReadBaseConfigFile(t *testing.T) {
	c, err := ReadBaseConfigFile("../../config/base-config.yaml")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%#v\n", c)
}

func TestReadRegionConfigFile(t *testing.T) {
	c, err := ReadRegionConfigFile("../../conf/replica-config.yaml")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%#v\n", c)
}
