package server

import (
	"TikCache/engine"
	"TikCache/pack/proto"
)

type Server struct {
	engine.Engine
	proto proto.Proto
}

func New(opt *Option) *Server {
	s := &Server{}
	switch opt.EngineType {
	case "caches":
		s.Engine = engine.NewEngine("caches")
	default:
		panic("invalid engine type")
	}

	switch {

	}

	return s
}
