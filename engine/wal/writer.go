package wal

import (
	"encoding/binary"
	"os"
)

type Writer struct {
	file         string // 预先日志文件
	dest         *os.File
	assistBuffer [30]byte
}

func NewWriter(file string) (*Writer, error) {
	dest, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file: file,
		dest: dest,
	}, nil
}

func (w *Writer) Write(key, value []byte) error {
	n := binary.PutUvarint(w.assistBuffer[0:], uint64(len(key)))
	n += binary.PutUvarint(w.assistBuffer[n:], uint64(len(value)))

	var b []byte

	b = append(b, w.assistBuffer[:n]...)
	b = append(b, key...)
	b = append(b, value...)

	_, err := w.dest.Write(b)
	return err
}

func (w *Writer) Close() {
	_ = w.dest.Close()
}
