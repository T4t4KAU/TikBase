package http

import (
	"TikBase/engine/values"
	"TikBase/iface"
	"TikBase/pack/net/http/router"
	"io"
	"net/http"
	"strconv"
)

func StartServer(address string, eng iface.Engine) error {
	return NewServer(eng).Run(address)
}

type Server struct {
	engine iface.Engine
}

func NewServer(eng iface.Engine) *Server {
	return &Server{
		engine: eng,
	}
}

func (s *Server) Run(address string) error {
	return http.ListenAndServe(address, s.routerHandler())
}

func (s *Server) routerHandler() *router.Router {
	r := router.New()
	r.GET("/store/:key", s.getHandler)
	r.PUT("/store/:key", s.setHandler)
	r.DELETE("/store/:key", s.deleteHandler)
	r.GET("/store/status", s.statusHandler)
	r.GET("/store/echo/:key", s.echoHandler)
	return r
}

func parseTTL(request *http.Request) (int64, error) {
	ttls, ok := request.Header["Ttl"]
	if !ok || len(ttls) < 1 {
		return values.NeverExpire, nil
	}
	return strconv.ParseInt(ttls[0], 10, 64)
}

func (s *Server) setHandler(ctx *router.Context) {
	key := ctx.Params.ByName("key")
	val, err := io.ReadAll(ctx.Req.Body)
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	args := [][]byte{[]byte(key), val}
	res := s.engine.Exec(iface.SET_STR, args)
	if !res.Success() {
		ctx.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	ctx.Writer.WriteHeader(http.StatusCreated)
}

func (s *Server) getHandler(ctx *router.Context) {
	key := ctx.Params.ByName("key")
	res := s.engine.Exec(iface.GET_STR, [][]byte{[]byte(key)})
	if !res.Success() {
		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = ctx.Writer.Write(res.Data()[0])
}

func (s *Server) deleteHandler(ctx *router.Context) {
	key := ctx.Params.ByName("key")
	res := s.engine.Exec(iface.DEL, [][]byte{[]byte(key)})
	if !res.Success() {
		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}
	ctx.Writer.WriteHeader(http.StatusOK)
}

func (s *Server) statusHandler(ctx *router.Context) {
	ctx.Writer.WriteHeader(http.StatusOK)
}

func (s *Server) echoHandler(ctx *router.Context) {
	key := ctx.Params.ByName("key")
	res := s.engine.Exec(iface.ECHO, [][]byte{[]byte(key)})
	if !res.Success() {
		ctx.Writer.WriteHeader(http.StatusBadGateway)
	}
	_, _ = ctx.Writer.Write(res.Data()[0])
}
