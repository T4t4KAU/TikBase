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
	Try(ctx context.Context, req *TccReq) (*TccResp, error)
	Confirm(ctx context.Context, txId string) (*TccResp, error)
	Cancel(ctx context.Context, txId string) (*TccResp, error)
}
