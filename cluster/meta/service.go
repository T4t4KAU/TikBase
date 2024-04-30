package meta

import (
	"context"
	"encoding/json"
	"github.com/T4t4KAU/TikBase/cluster/replica"
	"github.com/T4t4KAU/TikBase/cluster/slice"
	"github.com/T4t4KAU/TikBase/pkg/consts"
	meta0 "github.com/T4t4KAU/TikBase/pkg/rpc/meta"
	"github.com/T4t4KAU/TikBase/pkg/utils"
)

// Service implements the last service interface defined in the IDL.
type Service struct {
	sc slice.Slice
	re replica.Service
}

func (s *Service) Start() error {
	//TODO implement me
	panic("implement me")
}

func (s *Service) Name() string {
	return consts.MetaServiceName
}

// RegionList implements the Service interface.
func (s *Service) RegionList(ctx context.Context, req *meta0.RegionListReq) (resp *meta0.RegionListResp, err error) {
	resp = &meta0.RegionListResp{}

	nodes := s.sc.Nodes()
	bytes, err := json.Marshal(nodes)
	if err != nil {
		resp.Message = err.Error()
		return resp, err
	}

	resp.Message = utils.B2S(bytes)

	return
}

// RegionStatus implements the Service interface.
func (s *Service) RegionStatus(ctx context.Context, req *meta0.RegionStatusReq) (resp *meta0.RegionStatusResp, err error) {
	// TODO: Your code here...
	return
}

// ReplicaList implements the Service interface.
func (s *Service) ReplicaList(ctx context.Context, req *meta0.ReplicaListReq) (resp *meta0.ReplicaListResp, err error) {
	resp.Message = s.re.ReplicaList()

	return
}

// ReplicaStatus implements the Service interface.
func (s *Service) ReplicaStatus(ctx context.Context, req *meta0.ReplicaStatusReq) (resp *meta0.ReplicaStatusResp, err error) {
	// TODO: Your code here...
	return
}
