package txn

import (
	"context"
	"time"
)

type TXStore interface {
	CreateTX(ctx context.Context, components ...TccComponent) (txID string, err error)
	TXUpdate(ctx context.Context, txID string, componentID string, accept bool) error
	TXSubmit(ctx context.Context, txID string, success bool) error
	GetHangingTXs(ctx context.Context) ([]*Transaction, error)
	GetTX(ctx context.Context, txID string) (*Transaction, error)
	Lock(ctx context.Context, expireDuration time.Duration) error
	Unlock(ctx context.Context) error
}
