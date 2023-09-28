package http

import (
	"TikCache/engine"
	"TikCache/engine/caches"
	"TikCache/pack/proto/http/router"
	"net/http"
	"strconv"
)

type Server struct {
	engine engine.Engine
}

func NewServer(eng engine.Engine) *Server {
	return &Server{
		engine: eng,
	}
}

func (s *Server) Run(address string) error {
	return http.ListenAndServe(address, s.routerHandler())
}

func (s *Server) routerHandler() *router.Router {
	r := router.New()
	r.GET("/cache/:key", s.getHandler)
	r.PUT("/cache/:key", s.setHandler)
	r.DELETE("/cache/:key", s.deleteHandler)
	r.GET("/status", s.statusHandler)
	return r
}

func (s *Server) setHandler(ctx *router.Context) {

}

func parseTTL(request *http.Request) (int64, error) {
	ttls, ok := request.Header["Ttl"]
	if !ok || len(ttls) < 1 {
		return caches.NeverExpire, nil
	}
	return strconv.ParseInt(ttls[0], 10, 64)
}

func (s *Server) getHandler(ctx *router.Context) {

}

func (s *Server) deleteHandler(ctx *router.Context) {

}

func (s *Server) statusHandler(ctx *router.Context) {

}
