package net

import (
	"TikCache/mode/caches"
	"TikCache/net/http"
	"TikCache/net/tcp"
)

type Server interface {
	Run(address string) error
}

func NewServer(serverType string, cache *caches.Cache) Server {
	if serverType == "http" {
		return http.NewServer(cache)
	}
	return tcp.NewServer(cache)
}
