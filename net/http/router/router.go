package router

import (
	"net/http"
	"strings"
)

// HTTP请求路径由'/'分隔的多段构成
// 每一段可以作为前缀树的一个节点
// 通过树结构查询 如果中间某一层节点都不满足条件 即未匹配到路由

type HandlerFunc func(*Context)

type Param struct {
	Key   string
	Value string
}
type Params []Param

func (ps Params) ByName(name string) string {
	for _, p := range ps {
		if p.Key == name {
			return p.Value
		}
	}
	return ""
}

type Router struct {
	roots    map[string]*node
	handlers map[string]HandlerFunc
}

func New() *Router {
	return newRouter()
}

func newRouter() *Router {
	return &Router{
		roots:    make(map[string]*node),
		handlers: make(map[string]HandlerFunc),
	}
}

func parsePattern(pattern string) []string {
	vs := strings.Split(pattern, "/")

	parts := make([]string, 0)
	for _, item := range vs {
		if item != "" {
			parts = append(parts, item)
			if item[0] == '*' {
				break
			}
		}
	}
	return parts
}

func (r *Router) GET(pattern string, handler HandlerFunc) {
	r.addRoute(http.MethodGet, pattern, handler)
}

func (r *Router) HEAD(pattern string, handler HandlerFunc) {
	r.addRoute(http.MethodHead, pattern, handler)
}

func (r *Router) POST(pattern string, handler HandlerFunc) {
	r.addRoute(http.MethodPost, pattern, handler)
}

func (r *Router) PUT(pattern string, handler HandlerFunc) {
	r.addRoute(http.MethodPut, pattern, handler)
}

func (r *Router) DELETE(pattern string, handler HandlerFunc) {
	r.addRoute(http.MethodDelete, pattern, handler)
}

// 添加路由
func (r *Router) addRoute(method string, pattern string, handler HandlerFunc) {
	parts := parsePattern(pattern)
	key := method + "-" + pattern
	_, ok := r.roots[method]
	if !ok {
		r.roots[method] = &node{}
	}
	r.roots[method].insert(pattern, parts, 0)
	r.handlers[key] = handler
}

func (r *Router) getRoute(method string, path string) (*node, Params) {
	searchParts := parsePattern(path)
	params := make([]Param, 0)
	root, ok := r.roots[method]
	if !ok {
		return nil, nil
	}
	n := root.search(searchParts, 0)
	if n != nil {
		parts := parsePattern(n.pattern)
		for index, part := range parts {
			if part[0] == ':' {
				params = append(params, Param{part[1:], searchParts[index]})
			}
			if part[0] == '*' && len(part) > 1 {
				params = append(params, Param{part[1:],
					strings.Join(searchParts[index:], "/")})
				break
			}
		}
		return n, params
	}
	return nil, nil
}

func (r *Router) getRoutes(method string) []*node {
	root, ok := r.roots[method]
	if !ok {
		return nil
	}
	nodes := make([]*node, 0)
	root.travel(&nodes)
	return nodes
}

func (r *Router) handle(ctx *Context) {
	n, params := r.getRoute(ctx.Method, ctx.Path)
	if n != nil {
		ctx.Params = params
		key := ctx.Method + "-" + n.pattern
		r.handlers[key](ctx)
	} else {
		ctx.String(http.StatusNotFound, "%s", ctx.Path)
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := newContext(w, req)
	r.handle(ctx)
}
