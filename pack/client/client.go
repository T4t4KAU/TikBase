package client

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pack/errno"
	"github.com/T4t4KAU/TikBase/pack/net/resp"
	"github.com/T4t4KAU/TikBase/pack/net/tiko"
	"github.com/T4t4KAU/TikBase/pack/utils"
	"net"
	"strings"
)

var protos = map[string]struct{}{
	"tiko": {},
	"resp": {},
}

func newClient(proto string, conn net.Conn) iface.Client {
	proto = strings.ToLower(proto)

	switch proto {
	case "tiko":
		return tiko.NewClient(conn)
	case "resp":
		return resp.NewClient(conn)
	default:
		panic("invalid protocol")
	}
}

type Client struct {
	Protocol string
	Addr     string
	client   iface.Client
}

func New(addr, proto string) (*Client, error) {
	if !utils.ValidateAddress(addr) {
		return &Client{}, errno.ErrInvalidAddress
	}
	if _, ok := protos[proto]; !ok {
		return &Client{}, errno.ErrInvalidProtocol
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return &Client{}, err
	}

	return &Client{
		Protocol: proto,
		Addr:     addr,
		client:   newClient(proto, conn),
	}, nil
}

func (c *Client) Set(key, value string) error {
	return c.client.Set(key, value)
}

func (c *Client) Get(key string) (string, error) {
	return c.client.Get(key)
}

func (c *Client) Del(key string) error {
	return c.client.Del(key)
}

func (c *Client) Expire(key string, ttl int64) error {
	return c.client.Expire(key, ttl)
}
