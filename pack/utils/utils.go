package utils

import (
	"bytes"
	"encoding/binary"
	"time"
)

// Copy 数据复制
func Copy(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func CopyTo(dst []byte, src []byte) {
	copy(dst, src)
}

func IntToBytes(e int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(e))
	return b
}

func BytesToInt(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func CompareKey(a, b string) int {
	return bytes.Compare([]byte(a), []byte(b))
}

// NowSuffix 当前时间戳后缀
func NowSuffix() string {
	return "." + time.Now().Format("20060102150405")
}
