package resp

import "io"

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

func writeGetRequest(writer io.Writer, key []byte) (int, error) {
	return writer.Write(MakeGetRequest(key).ToBytes())
}

func writeSetRequest(writer io.Writer, key []byte, value []byte) (int, error) {
	return writer.Write(MakeSetRequest(key, value).ToBytes())
}
