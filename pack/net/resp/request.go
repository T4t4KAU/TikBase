package resp

import (
	"github.com/T4t4KAU/TikBase/pack/utils"
	"io"
)

type GetRequest struct {
	Key []byte
}

func MakeGetRequest(key []byte) *GetRequest {
	return &GetRequest{
		Key: key,
	}
}

func (req *GetRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("GET"), req.Key}).ToBytes()
}

type SetRequest struct {
	Key   []byte
	Value []byte
}

func MakeSetRequest(key, value []byte) *SetRequest {
	return &SetRequest{
		Key:   key,
		Value: value,
	}
}

func (req *SetRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("SET"), req.Key, req.Value}).ToBytes()
}

type DelRequest struct {
	Key []byte
}

func MakeDelRequest(key []byte) *DelRequest {
	return &DelRequest{
		Key: key,
	}
}

func (req *DelRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("DEL"), req.Key}).ToBytes()
}

func writeGetRequest(writer io.Writer, key []byte) (int, error) {
	return writer.Write(MakeGetRequest(key).ToBytes())
}

func writeSetRequest(writer io.Writer, key []byte, value []byte) (int, error) {
	return writer.Write(MakeSetRequest(key, value).ToBytes())
}

func writeDelRequest(writer io.Writer, key []byte) (int, error) {
	return writer.Write(MakeDelRequest(key).ToBytes())
}

type ExpireRequest struct {
	Key []byte
	TTL int64
}

func MakeExpireRequest(key []byte, ttl int64) *ExpireRequest {
	return &ExpireRequest{
		Key: key,
		TTL: ttl,
	}
}

func (req *ExpireRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("EXPIRE"), req.Key, utils.I642B(req.TTL)}).ToBytes()
}

func writeExpireRequest(writer io.Writer, key []byte, ttl int64) (int, error) {
	return writer.Write(MakeExpireRequest(key, ttl).ToBytes())
}
