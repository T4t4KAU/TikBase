package main

import (
	"context"
	consis "github.com/T4t4KAU/TikBase/pkg/rpc/consis"
)

// ConsisServiceImpl implements the last service interface defined in the IDL.
type ConsisServiceImpl struct{}

// Join implements the ConsisServiceImpl interface.
func (s *ConsisServiceImpl) Join(ctx context.Context, req *consis.JoinReq) (resp *consis.JoinResp, err error) {
	// TODO: Your code here...
	return
}
