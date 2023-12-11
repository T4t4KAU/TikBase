package web

import (
	"TikBase/iface"
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func StartServer(eng iface.Engine) {
	s := server.New(
		server.WithHostPorts("0.0.0.0:8888"),
		server.WithHandleMethodNotAllowed(true),
		server.WithMaxKeepBodySize(1024*1024*1024*1024),
		server.WithMaxRequestBodySize(1024*1024*1024*1024),
	)

	h := NewHandler(eng)

	router := s.Group("/store")
	router.GET("/:key", h.GetHandler)
	router.PUT("/:key/:value", h.SetHandler)
	router.DELETE("/:key", h.DelHandler)

	s.NoRoute(func(ctx context.Context, c *app.RequestContext) {
		c.String(consts.StatusOK, "no route")
	})
	s.NoMethod(func(ctx context.Context, c *app.RequestContext) {
		c.String(consts.StatusOK, "no method")
	})

	s.Spin()
}
