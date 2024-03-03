package data

import (
	"context"
	"github.com/T4t4KAU/TikBase/cluster/slice"
	"github.com/T4t4KAU/TikBase/iface"
	data "github.com/T4t4KAU/TikBase/pkg/rpc/data"
	"github.com/T4t4KAU/TikBase/pkg/rpc/data/dataservice"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/cloudwego/kitex/client"
)

/// 数据服务 处理数据请求

// Service implements the last service interface defined in the IDL.
type Service struct {
	slice *slice.Slice
}

func (s *Service) Start() error {
	//TODO implement me
	panic("implement me")
}

func (s *Service) Name() string {
	return "data-service"
}

// Get implements the Service interface.
func (s *Service) Get(ctx context.Context, req *data.GetReq) (resp *data.GetResp, err error) {

	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		resp.Message = err.Error()
		return
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectGet(ctx, req, node)
	}

	resp = new(data.GetResp)

	res := s.slice.Exec(iface.GET_STR, utils.KeyBytes(req.Key))
	resp.Message = res.Error().Error()
	resp.Success = res.Success()
	resp.Value = res.Data()

	return
}

// Set implements the Service interface.
func (s *Service) Set(ctx context.Context, req *data.SetReq) (resp *data.SetResp, err error) {

	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		resp.Message = err.Error()
		return
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectSet(ctx, req, node)
	}

	resp = new(data.SetResp)
	res := s.slice.Exec(iface.SET_STR, utils.KeyValueBytes(req.Key, req.Value))
	resp.Message = res.Error().Error()
	resp.Success = res.Success()

	return
}

// Del implements the Service interface.
func (s *Service) Del(ctx context.Context, req *data.DelReq) (resp *data.DelResp, err error) {
	resp = new(data.DelResp)

	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		resp.Message = err.Error()
		return
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectDel(ctx, req, node)
	}

	res := s.slice.Exec(iface.DEL, utils.KeyBytes(req.Key))
	resp.Message = res.Error().Error()
	resp.Success = res.Success()

	return
}

// HSet implements the Service interface.
func (s *Service) HSet(ctx context.Context, req *data.HSetReq) (resp *data.HSetResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		resp.Message = err.Error()
		return
	}

	if !s.slice.IsCurrentNode(node) {
		c, err := dataservice.NewClient(node, client.WithHostPorts(node))
		if err != nil {
			return
		}

		r, err := c.HSet(ctx, req)
		if err != nil {
			return
		}

		resp.Success = r.Success
		resp.Message = r.Message
	}

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

func (s *Service) RedirectGet(ctx context.Context, req *data.GetReq, node string) (resp *data.GetResp, err error) {
	resp = new(data.GetResp)

	c, err := dataservice.NewClient(node, client.WithHostPorts(node))
	if err != nil {
		return
	}

	r, err := c.Get(ctx, req)
	if err != nil {
		return
	}

	resp.Success = r.Success
	resp.Message = r.Message
	resp.Value = r.Value

	return
}

func (s *Service) RedirectSet(ctx context.Context, req *data.SetReq, node string) (resp *data.SetResp, err error) {
	resp = new(data.SetResp)

	c, err := dataservice.NewClient(node, client.WithHostPorts(node))
	if err != nil {
		return
	}

	r, err := c.Set(ctx, req)
	if err != nil {
		return
	}

	resp.Success = r.Success
	resp.Message = r.Message

	return
}

func (s *Service) RedirectDel(ctx context.Context, req *data.DelReq, node string) (resp *data.DelResp, err error) {
	resp = new(data.DelResp)

	c, err := dataservice.NewClient(node, client.WithHostPorts(node))
	if err != nil {
		return
	}

	r, err := c.Del(ctx, req)
	if err != nil {
		return
	}

	resp.Success = r.Success
	resp.Message = r.Message

	return
}
