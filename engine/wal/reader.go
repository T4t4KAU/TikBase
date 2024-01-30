package wal

import (
	"bufio"
	"os"
)

type Reader struct {
	path   string
	src    *os.File
	reader *bufio.Reader
}

func NewReader(path string) (*Reader, error) {
	src, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &Reader{
		path:   "",
		src:    src,
		reader: bufio.NewReader(src),
	}, nil
}
