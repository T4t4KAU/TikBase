package web

import (
	"TikBase/iface"
	"context"
	"github.com/cloudwego/hertz/pkg/app"
)

type Handler struct {
	eng iface.Engine
}

func NewHandler(eng iface.Engine) *Handler {
	return &Handler{
		eng: eng,
	}
}

func (h *Handler) GetHandler(ctx context.Context, c *app.RequestContext) {

}

func (h *Handler) SetHandler(ctx context.Context, c *app.RequestContext) {

}

func (h *Handler) DelHandler(ctx context.Context, c *app.RequestContext) {

}
