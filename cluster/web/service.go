package web

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/consts"
	"github.com/T4t4KAU/TikBase/pkg/net/http"
)

// Service Web服务
type Service struct {
	Address string       // 服务地址
	eng     iface.Engine // 存储引擎
}

// NewService 创建Web服务
func NewService(addr string, eng iface.Engine) *Service {
	return &Service{
		Address: addr,
		eng:     eng,
	}
}

func (s *Service) Name() string {
	return consts.WebServiceName
}

// Start 启动服务
func (s *Service) Start() error {
	return http.StartServer(s.Address, s.eng)
}
