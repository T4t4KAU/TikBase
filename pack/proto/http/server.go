package http

import (
	"TikCache/engine/caches"
	router2 "TikCache/pack/proto/http/router"
	"encoding/json"
	"io"
	"net/http"
	"path"
	"strconv"
)

const (
	APIVersion = "v1"
)

type Server struct {
	*caches.Cache
}

func NewServer(cache *caches.Cache) *Server {
	return &Server{
		Cache: cache,
	}
}

func (s *Server) Run(address string) error {
	return http.ListenAndServe(address, s.routerHandler())
}

func wrapUriWithVersion(uri string) string {
	return path.Join("/", APIVersion, uri)
}

func (s *Server) routerHandler() *router2.Router {
	r := router2.New()
	r.GET(wrapUriWithVersion("/cache/:key"), s.getHandler)
	r.PUT(wrapUriWithVersion("/cache/:key"), s.setHandler)
	r.DELETE(wrapUriWithVersion("/cache/:key"), s.deleteHandler)
	r.GET(wrapUriWithVersion("/status"), s.statusHandler)
	return r
}

func (s *Server) setHandler(ctx *router2.Context) {
	// 查找指定key
	key := ctx.Params.ByName("key")
	value, err := io.ReadAll(ctx.Req.Body)
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 解析存活时间
	ttl, err := parseTTL(ctx.Req)
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 设置存活时间
	err = s.SetWithTTL(key, value, ttl)
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusRequestEntityTooLarge)
		ctx.Writer.Write([]byte("Error: " + err.Error()))
		return
	}
	ctx.Writer.WriteHeader(http.StatusCreated)
}

func parseTTL(request *http.Request) (int64, error) {
	ttls, ok := request.Header["Ttl"]
	if !ok || len(ttls) < 1 {
		return caches.NeverExpire, nil
	}
	return strconv.ParseInt(ttls[0], 10, 64)
}

func (s *Server) getHandler(ctx *router2.Context) {
	key := ctx.Params.ByName("key")
	value, ok := s.Get(key)
	if !ok {
		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}
	ctx.Writer.Write(value)
}

func (s *Server) deleteHandler(ctx *router2.Context) {
	key := ctx.Params.ByName("key")
	err := s.Delete(key)
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) statusHandler(ctx *router2.Context) {
	status, err := json.Marshal(s.Status())
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	ctx.Writer.Write(status)
}
