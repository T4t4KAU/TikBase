package txn

import (
	"context"
)

type TxManager struct {
	ctx  context.Context
	stop context.CancelFunc
}
