package bases

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDB(base *Base) {
	if base != nil {
		_ = base.activeFile.Close()
		err := os.RemoveAll(base.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestNew(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "tikbase")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	// defer destroyDB(b)

	assert.Nil(t, err)
	assert.NotNil(t, b)
}

func TestBase_Set(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "tikbase")
	opts.DirPath = dir
	b, err := NewBaseWith(opts)
	assert.Nil(t, err)
	assert.NotNil(t, b)

	v := values.New([]byte(utils.GenerateRandomString(10)), 0, iface.STRING)
	res := b.Set("test", &v)
	assert.True(t, res)

	for i := 0; i < 100000; i++ {
		v = values.New([]byte(utils.GenerateRandomString(10)), 0, iface.STRING)
		res = b.Set(utils.GenerateRandomString(10), &v)
		assert.True(t, res)
	}
}
