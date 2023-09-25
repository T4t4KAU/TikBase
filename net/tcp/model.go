package tcp

import "encoding/json"

// Status 状态
type Status struct {
	Count     int `json:"count"` //
	KeySize   int `json:"leySize"`
	ValueSize int `json:"valueSize"`
}

// 请求
type request struct {
	command byte
	args    [][]byte
	resChan chan *Response
}

// Response 响应
type Response struct {
	Body []byte
	Err  error
}

// ToStatus 响应状态
func (r *Response) ToStatus() (*Status, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	status := &Status{}
	return status, json.Unmarshal(r.Body, status)
}
