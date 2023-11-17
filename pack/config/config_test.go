package config

import "testing"

func TestReadConfigFile(t *testing.T) {
	c, err := ReadConfigFile("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(c)
}
