// Code generated by Kitex v0.6.2. DO NOT EDIT.
package metaservice

import (
	meta "github.com/T4t4KAU/TikBase/pkg/rpc/meta"
	server "github.com/cloudwego/kitex/server"
)

// NewServer creates a server.Server with the given handler and options.
func NewServer(handler meta.MetaService, opts ...server.Option) server.Server {
	var options []server.Option

	options = append(options, opts...)

	svr := server.NewServer(options...)
	if err := svr.RegisterService(serviceInfo(), handler); err != nil {
		panic(err)
	}
	return svr
}
