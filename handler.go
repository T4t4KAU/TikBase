package main

import (
	"context"
	data "github.com/T4t4KAU/TikBase/pkg/rpc/data"
)

// DataServiceImpl implements the last service interface defined in the IDL.
type DataServiceImpl struct{}

// Get implements the DataServiceImpl interface.
func (s *DataServiceImpl) Get(ctx context.Context, req *data.GetReq) (resp *data.GetResp, err error) {
	// TODO: Your code here...
	return
}

// Set implements the DataServiceImpl interface.
func (s *DataServiceImpl) Set(ctx context.Context, req *data.SetReq) (resp *data.SetResp, err error) {
	// TODO: Your code here...
	return
}

// Del implements the DataServiceImpl interface.
func (s *DataServiceImpl) Del(ctx context.Context, req *data.DelReq) (resp *data.DelResp, err error) {
	// TODO: Your code here...
	return
}

// HSet implements the DataServiceImpl interface.
func (s *DataServiceImpl) HSet(ctx context.Context, req *data.HSetReq) (resp *data.HSetResp, err error) {
	// TODO: Your code here...
	return
}

// HGet implements the DataServiceImpl interface.
func (s *DataServiceImpl) HGet(ctx context.Context, req *data.HGetReq) (resp *data.HGetResp, err error) {
	// TODO: Your code here...
	return
}

// HDel implements the DataServiceImpl interface.
func (s *DataServiceImpl) HDel(ctx context.Context, req *data.HDelReq) (resp *data.HDelResp, err error) {
	// TODO: Your code here...
	return
}

// LPush implements the DataServiceImpl interface.
func (s *DataServiceImpl) LPush(ctx context.Context, req *data.LPushReq) (resp *data.LPushResp, err error) {
	// TODO: Your code here...
	return
}

// RPush implements the DataServiceImpl interface.
func (s *DataServiceImpl) RPush(ctx context.Context, req *data.RPushReq) (resp *data.RPushResp, err error) {
	// TODO: Your code here...
	return
}

// LPop implements the DataServiceImpl interface.
func (s *DataServiceImpl) LPop(ctx context.Context, req *data.LPopReq) (resp *data.LPopResp, err error) {
	// TODO: Your code here...
	return
}

// RPop implements the DataServiceImpl interface.
func (s *DataServiceImpl) RPop(ctx context.Context, req *data.RPopReq) (resp *data.RPopResp, err error) {
	// TODO: Your code here...
	return
}

// SAdd implements the DataServiceImpl interface.
func (s *DataServiceImpl) SAdd(ctx context.Context, req *data.SAddReq) (resp *data.SAddResp, err error) {
	// TODO: Your code here...
	return
}

// SRem implements the DataServiceImpl interface.
func (s *DataServiceImpl) SRem(ctx context.Context, req *data.SRemReq) (resp *data.SRemResp, err error) {
	// TODO: Your code here...
	return
}
