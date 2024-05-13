package slice

import "github.com/T4t4KAU/TikBase/pkg/consts"

type Service struct {
}

func (s *Service) Start() error {
	//TODO implement me
	panic("implement me")
}

func (s *Service) Name() string {
	return consts.SliceServiceName
}
