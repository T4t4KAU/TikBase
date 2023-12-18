package utils

import (
	"bytes"
	"encoding/binary"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Copy 数据复制
func Copy(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func CopyTo(dst []byte, src []byte) {
	copy(dst, src)
}

func CompareKey(a, b string) int {
	return bytes.Compare([]byte(a), []byte(b))
}

// NowSuffix 当前时间戳后缀
func NowSuffix() string {
	return "." + time.Now().Format("20060102150405")
}

func S2B(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Len = sh.Len
	bh.Cap = sh.Len
	return b
}

func B2S(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func I2B(e int) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(e))
	return b
}

func B2I(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func I642B(e int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(e))
	return b
}

func B2I64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func B2F64(b []byte) float64 {
	val, _ := strconv.ParseFloat(B2S(b), 64)
	return val
}

func F642B(f float64) []byte {
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}

func GenerateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}

	return string(b)
}

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取磁盘空间大小 字节为单位
func AvailableDiskSize() (uint64, error) {
	var stat syscall.Statfs_t

	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func CopyDir(src, dest string, exclude []string) error {
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err = os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
