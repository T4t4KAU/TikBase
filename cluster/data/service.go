package data

import (
	"context"
	"github.com/T4t4KAU/TikBase/cluster/replica/raft"
	"github.com/T4t4KAU/TikBase/cluster/slice"
	"github.com/T4t4KAU/TikBase/engine"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/consts"
	"github.com/T4t4KAU/TikBase/pkg/rpc/data"
	"github.com/T4t4KAU/TikBase/pkg/rpc/data/dataservice"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"net"
)

/// 数据服务 处理数据请求

// Service implements the last service interface defined in the IDL.
type Service struct {
	address string
	slice   *slice.Slice
	peer    *raft.Peer
}

func NewService(sc *slice.Slice, addr string, peer *raft.Peer) *Service {
	return &Service{
		slice:   sc,
		address: addr,
		peer:    peer,
	}
}

func (s *Service) Start() error {
	addr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	srv := dataservice.NewServer(s,
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: s.Name()}),
		server.WithServiceAddr(addr),
	)

	klog.Infof("start data service at %s", s.address)

	return srv.Run()
}

func (s *Service) Name() string {
	return consts.DataServiceName
}

// Get implements the Service interface.
func (s *Service) Get(ctx context.Context, req *data.GetReq) (resp *data.GetResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.GetResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectGet(ctx, req, node)
	}

	resp = new(data.GetResp)

	// 执行指令
	res := s.slice.Exec(iface.GET_STR, utils.KeyBytes(req.Key))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()
	resp.Value = res.Data()

	return
}

// Set implements the Service interface.
func (s *Service) Set(ctx context.Context, req *data.SetReq) (resp *data.SetResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.SetResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectSet(ctx, req, node)
	}

	resp = new(data.SetResp)
	res := s.slice.Exec(iface.SET_STR, utils.KeyValueBytes(req.Key, req.Value))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	if e := s.peer.Apply(iface.Command{
		Ins:   iface.SET_STR,
		Key:   req.Key,
		Value: req.Value,
	}); e != nil {
		klog.Error("failed to apply command: ", e)
	} else {
		klog.Infof("apply command ok")
	}

	return
}

// Del implements the Service interface.
func (s *Service) Del(ctx context.Context, req *data.DelReq) (resp *data.DelResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.DelResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectDel(ctx, req, node)
	}

	resp = new(data.DelResp)
	res := s.slice.Exec(iface.DEL, utils.KeyBytes(req.Key))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	if e := s.peer.Apply(iface.Command{
		Ins: iface.SET_STR,
		Key: req.Key,
	}); e != nil {
		klog.Error("failed to apply command: ", e)
	} else {
		klog.Infof("apply command ok")
	}

	return
}

// Expire implements the Service interface.
func (s *Service) Expire(ctx context.Context, req *data.ExpireReq) (resp *data.ExpireResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.ExpireResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectExpire(ctx, req, node)
	}

	resp = new(data.ExpireResp)
	res := s.slice.Exec(iface.EXPIRE, utils.KeyBytes(req.Key))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// HSet implements the Service interface.
func (s *Service) HSet(ctx context.Context, req *data.HSetReq) (resp *data.HSetResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.HSetResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectHSet(ctx, req, node)
	}

	resp = new(data.HSetResp)
	res := s.slice.Exec(iface.SET_HASH, engine.MakeHashSetArgs(req.Key, req.Field, req.Value))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// HGet implements the Service interface.
func (s *Service) HGet(ctx context.Context, req *data.HGetReq) (resp *data.HGetResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.HGetResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectHGet(ctx, req, node)
	}

	resp = new(data.HGetResp)
	res := s.slice.Exec(iface.GET_HASH, engine.MakeHashGetArgs(req.Key, req.Field))
	resp.Success = res.Success()
	resp.Message = utils.WithMessage(res.Error())
	resp.Value = res.Data()

	return
}

// HDel implements the Service interface.
func (s *Service) HDel(ctx context.Context, req *data.HDelReq) (resp *data.HDelResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.HDelResp{
			Message: err.Error(),
			Success: false,
		}, err
	}
	if !s.slice.IsCurrentNode(node) {
		return s.RedirectHDel(ctx, req, node)
	}

	resp = new(data.HDelResp)
	res := s.slice.Exec(iface.DEL_HASH, engine.MakeHashDelArgs(req.Key, req.Field))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// LPush implements the Service interface.
func (s *Service) LPush(ctx context.Context, req *data.LPushReq) (resp *data.LPushResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.LPushResp{
			Message: err.Error(),
			Success: false,
		}, err
	}
	if !s.slice.IsCurrentNode(node) {
		return s.RedirectLPush(ctx, req, node)
	}

	resp = new(data.LPushResp)
	res := s.slice.Exec(iface.LEFT_PUSH_LIST, engine.MakeListPushArgs(req.Key, req.Element))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// RPush implements the Service interface.
func (s *Service) RPush(ctx context.Context, req *data.RPushReq) (resp *data.RPushResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		resp.Message = err.Error()
		return
	}
	if !s.slice.IsCurrentNode(node) {
		return s.RedirectRPush(ctx, req, node)
	}

	resp = new(data.RPushResp)
	res := s.slice.Exec(iface.RIGHT_PUSH_LIST, engine.MakeListPushArgs(req.Key, req.Element))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// LPop implements the Service interface.
func (s *Service) LPop(ctx context.Context, req *data.LPopReq) (resp *data.LPopResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.LPopResp{
			Message: err.Error(),
			Success: false,
		}, err
	}
	if !s.slice.IsCurrentNode(node) {
		return s.RedirectLPop(ctx, req, node)
	}

	resp = new(data.LPopResp)
	res := s.slice.Exec(iface.LEFT_POP_LIST, engine.MakeListPopArgs(req.Key))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// RPop implements the Service interface.
func (s *Service) RPop(ctx context.Context, req *data.RPopReq) (resp *data.RPopResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.RPopResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectRPop(ctx, req, node)
	}

	resp = new(data.RPopResp)
	res := s.slice.Exec(iface.RIGHT_POP_LIST, engine.MakeListPopArgs(req.Key))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// SAdd implements the Service interface.
func (s *Service) SAdd(ctx context.Context, req *data.SAddReq) (resp *data.SAddResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.SAddResp{
			Message: err.Error(),
			Success: false,
		}, err
	}

	if !s.slice.IsCurrentNode(node) {
		return s.RedirectSAdd(ctx, req, node)
	}

	resp = new(data.SAddResp)
	res := s.slice.Exec(iface.ADD_SET, engine.MakeSetAddArgs(req.Key, req.Element))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// SRem implements the Service interface.
func (s *Service) SRem(ctx context.Context, req *data.SRemReq) (resp *data.SRemResp, err error) {
	node, err := s.slice.SelectNode(req.Key)
	if err != nil {
		return &data.SRemResp{
			Message: err.Error(),
			Success: false,
		}, err
	}
	if !s.slice.IsCurrentNode(node) {
		return s.RedirectSRem(ctx, req, node)
	}

	resp = new(data.SRemResp)
	res := s.slice.Exec(iface.REM_SET, engine.MakeSetRemArgs(req.Key, req.Element))
	resp.Message = utils.WithMessage(res.Error())
	resp.Success = res.Success()

	return
}

// ZAdd implements the Service interface.
func (s *Service) ZAdd(ctx context.Context, req *data.ZAddReq) (resp *data.ZAddResp, err error) {
	// TODO: Your code here...
	return
}

// ZRem implements the Service interface.
func (s *Service) ZRem(ctx context.Context, req *data.ZRemReq) (resp *data.ZRemResp, err error) {
	// TODO: Your code here...
	return
}

func (s *Service) RedirectGet(ctx context.Context, req *data.GetReq, node string) (resp *data.GetResp, err error) {
	resp = new(data.GetResp)

	resp.Success = false
	resp.Message = node
	resp.Value = nil
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectSet(ctx context.Context, req *data.SetReq, node string) (resp *data.SetResp, err error) {
	resp = new(data.SetResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectDel(ctx context.Context, req *data.DelReq, node string) (resp *data.DelResp, err error) {
	resp = new(data.DelResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectExpire(ctx context.Context, req *data.ExpireReq, node string) (resp *data.ExpireResp, err error) {
	resp = new(data.ExpireResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectHSet(ctx context.Context, req *data.HSetReq, node string) (resp *data.HSetResp, err error) {
	resp = new(data.HSetResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectHGet(ctx context.Context, req *data.HGetReq, node string) (resp *data.HGetResp, err error) {
	resp = new(data.HGetResp)

	resp.Success = false
	resp.Message = node
	resp.Value = nil
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectHDel(ctx context.Context, req *data.HDelReq, node string) (resp *data.HDelResp, err error) {
	resp = new(data.HDelResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectLPush(ctx context.Context, req *data.LPushReq, node string) (resp *data.LPushResp, err error) {
	resp = new(data.LPushResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect
	return
}

func (s *Service) RedirectRPush(ctx context.Context, req *data.RPushReq, node string) (resp *data.RPushResp, err error) {
	resp = new(data.RPushResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectLPop(ctx context.Context, req *data.LPopReq, node string) (resp *data.LPopResp, err error) {
	resp = new(data.LPopResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectRPop(ctx context.Context, req *data.RPopReq, node string) (resp *data.RPopResp, err error) {
	resp = new(data.RPopResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectSAdd(ctx context.Context, req *data.SAddReq, node string) (resp *data.SAddResp, err error) {
	resp = new(data.SAddResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}

func (s *Service) RedirectSRem(ctx context.Context, req *data.SRemReq, node string) (resp *data.SRemResp, err error) {
	resp = new(data.SRemResp)

	resp.Success = false
	resp.Message = node
	resp.StatusCode = consts.Redirect

	return
}
