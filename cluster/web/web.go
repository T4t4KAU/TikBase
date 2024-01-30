package web

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/net/http"
)

type Service struct {
	Address string
	eng     iface.Engine
}

func NewService(addr string, eng iface.Engine) *Service {
	return &Service{
		Address: addr,
		eng:     eng,
	}
}

func (s *Service) Start() error {
	return http.StartServer(s.Address, s.eng)
}
