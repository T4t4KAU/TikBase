package service

import (
	"github.com/T4t4KAU/TikBase/iface"
)

type Service struct {
	services map[string]iface.IService
}

func NewService(nodeId, addr string) *Service {
	svc := &Service{
		services: make(map[string]iface.IService),
	}

	return svc
}

func (s *Service) Start() {
	for _, svc := range s.services {
		go func(service iface.IService) {
			_ = service.Start()
		}(svc)
	}

	select {}
}

func (s *Service) Register(name string, service iface.IService) {
	s.services[name] = service
}

func (s *Service) Remove(name string) bool {
	if _, ok := s.services[name]; ok {
		delete(s.services, name)
		return true
	}
	return false
}
