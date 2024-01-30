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

func TestReadReplicaConfigFile(t *testing.T) {
	c, err := ReadReplicaConfigFile("../../conf/replica-config.yaml")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%#v\n", c)
}
