package consis

import (
	"github.com/T4t4KAU/TikBase/pkg/rpc/consis/consisservice"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"net"
)

func (s *Service) Start() error {
	addr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	svc := consisservice.NewServer(new(Service),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: s.Name()}),
		server.WithServiceAddr(addr),
	)

	return svc.Run()
}

func (s *Service) Name() string {
	return "consis-service"
}
