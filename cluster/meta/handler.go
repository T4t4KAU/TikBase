package meta

import (
	"context"
	meta0 "github.com/T4t4KAU/TikBase/pkg/rpc/meta"
)

// Service implements the last service interface defined in the IDL.
type Service struct{}

// RegionList implements the Service interface.
func (s *Service) RegionList(ctx context.Context, req *meta0.RegionListReq) (resp *meta0.RegionListResp, err error) {
	// TODO: Your code here...
	return
}

// RegionStatus implements the Service interface.
func (s *Service) RegionStatus(ctx context.Context, req *meta0.RegionStatusReq) (resp *meta0.RegionStatusResp, err error) {
	// TODO: Your code here...
	return
}
