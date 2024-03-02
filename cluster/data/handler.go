package data

import (
	"context"
	data "github.com/T4t4KAU/TikBase/pkg/rpc/data"
)

// Service implements the last service interface defined in the IDL.
type Service struct{}

// Get implements the Service interface.
func (s *Service) Get(ctx context.Context, req *data.GetReq) (resp *data.GetResp, err error) {
	// TODO: Your code here...
	return
}

// Set implements the Service interface.
func (s *Service) Set(ctx context.Context, req *data.SetReq) (resp *data.SetResp, err error) {
	// TODO: Your code here...
	return
}

// Del implements the Service interface.
func (s *Service) Del(ctx context.Context, req *data.DelReq) (resp *data.DelResp, err error) {
	// TODO: Your code here...
	return
}

// HSet implements the Service interface.
func (s *Service) HSet(ctx context.Context, req *data.HSetReq) (resp *data.HSetResp, err error) {
	// TODO: Your code here...
	return
}

// HGet implements the Service interface.
func (s *Service) HGet(ctx context.Context, req *data.HGetReq) (resp *data.HGetResp, err error) {
	// TODO: Your code here...
	return
}

// HDel implements the Service interface.
func (s *Service) HDel(ctx context.Context, req *data.HDelReq) (resp *data.HDelResp, err error) {
	// TODO: Your code here...
	return
}

// LPush implements the Service interface.
func (s *Service) LPush(ctx context.Context, req *data.LPushReq) (resp *data.LPushResp, err error) {
	// TODO: Your code here...
	return
}

// RPush implements the Service interface.
func (s *Service) RPush(ctx context.Context, req *data.RPushReq) (resp *data.RPushResp, err error) {
	// TODO: Your code here...
	return
}

// LPop implements the Service interface.
func (s *Service) LPop(ctx context.Context, req *data.LPopReq) (resp *data.LPopResp, err error) {
	// TODO: Your code here...
	return
}

// RPop implements the Service interface.
func (s *Service) RPop(ctx context.Context, req *data.RPopReq) (resp *data.RPopResp, err error) {
	// TODO: Your code here...
	return
}

// SAdd implements the Service interface.
func (s *Service) SAdd(ctx context.Context, req *data.SAddReq) (resp *data.SAddResp, err error) {
	// TODO: Your code here...
	return
}

// SRem implements the Service interface.
func (s *Service) SRem(ctx context.Context, req *data.SRemReq) (resp *data.SRemResp, err error) {
	// TODO: Your code here...
	return
}
