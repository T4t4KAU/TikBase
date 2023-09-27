package utils

import "encoding/binary"

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
