package txn

import "context"

type TccReq struct {
	ComponentID string
	TxID        string
	Data        map[string]any
}

type TccResp struct {
	ComponentID string
	Ack         bool
	TxID        string
}

type TccComponent interface {
	ID() string
	Try(ctx context.Context, req *TccReq) (*TccResp, error)     // 尝试
	Confirm(ctx context.Context, txId string) (*TccResp, error) // 确认
	Cancel(ctx context.Context, txId string) (*TccResp, error)  // 取消
}
