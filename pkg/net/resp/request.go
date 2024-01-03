package resp

import (
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

type GetRequest struct {
	Key []byte
}

func MakeGetRequest(key string) *GetRequest {
	return &GetRequest{
		Key: utils.S2B(key),
	}
}

func (req *GetRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("GET"), req.Key}).ToBytes()
}

type SetRequest struct {
	Key   []byte
	Value []byte
}

func MakeSetRequest(key, value string) *SetRequest {
	return &SetRequest{
		Key:   utils.S2B(key),
		Value: utils.S2B(value),
	}
}

func (req *SetRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("SET"), req.Key, req.Value}).ToBytes()
}

type DelRequest struct {
	Key []byte
}

func MakeDelRequest(key string) *DelRequest {
	return &DelRequest{
		Key: utils.S2B(key),
	}
}

func (req *DelRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("DEL"), req.Key}).ToBytes()
}

type ExpireRequest struct {
	Key []byte
	TTL int64
}

func MakeExpireRequest(key string, ttl int64) *ExpireRequest {
	return &ExpireRequest{
		Key: utils.S2B(key),
		TTL: ttl,
	}
}

func (req *ExpireRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("EXPIRE"), req.Key, utils.I642B(req.TTL)}).ToBytes()
}

type HSetRequest struct {
	Key   []byte
	Field []byte
	Value []byte
}

func MakeHSetRequest(key, field, value string) *HSetRequest {
	return &HSetRequest{
		Key:   utils.S2B(key),
		Field: utils.S2B(field),
		Value: utils.S2B(value),
	}
}

func (req *HSetRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("HSET"), req.Key, req.Field, req.Value}).ToBytes()
}

type HGetRequest struct {
	Key   []byte
	Field []byte
}

func MakeHGetRequest(key, field string) *HGetRequest {
	return &HGetRequest{
		Key:   utils.S2B(key),
		Field: utils.S2B(field),
	}
}

func (req *HGetRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("HGET"), req.Key, req.Field}).ToBytes()
}

type HDelRequest struct {
	Key   []byte
	Field []byte
	Value []byte
}

func MakeHDelRequest(key, field, value string) *HDelRequest {
	return &HDelRequest{
		Key:   utils.S2B(key),
		Field: utils.S2B(field),
		Value: utils.S2B(value),
	}
}

func (req *HDelRequest) ToBytes() []byte {
	return MakeMultiBulkReply([][]byte{[]byte("HDEL"), req.Key, req.Field, req.Value}).ToBytes()
}
