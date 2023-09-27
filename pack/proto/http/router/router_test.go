package router

import (
	"fmt"
	"testing"
)

func newTestRouter() *Router {
	r := newRouter()
	r.addRoute("GET", "/", nil)
	r.addRoute("GET", "/hello", nil)
	r.addRoute("GET", "/hello/:name", nil)
	r.addRoute("GET", "assets/*path", nil)
	return r
}

func TestGetRoute(t *testing.T) {
	r := newTestRouter()
	n, ps := r.getRoute("GET", "/hello/test")
	if n == nil {
		t.Fatal("nil shouldn't be returned")
	}
	if n.pattern != "/hello/:name" {
		t.Fatal("should match '/hello/:name'")
	}
	if ps.ByName("name") != "test" {
		t.Fatal("name should be equal to ''")
	}
	fmt.Printf("match path %s, params['name']: %v\n", n.pattern, ps)
}

func TestGetRoutes(t *testing.T) {
	r := newTestRouter()
	nodes := r.getRoutes("GET")
	for i, n := range nodes {
		fmt.Println(i+1, n)
	}
	if len(nodes) != 5 {
		t.Fatal("the number of routes should be 4")
	}
}
