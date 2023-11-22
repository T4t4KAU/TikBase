package config

import "testing"

func TestReadConfigFile(t *testing.T) {
	c, err := ReadConfigFile("../../config.yaml")
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(c)
}
