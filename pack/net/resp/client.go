package resp

import (
	"github.com/T4t4KAU/TikBase/pack/utils"
	"net"
	"strings"
)

type Client struct {
	conn net.Conn
}

func (c *Client) Del(key string) error {
	_, err := writeDelRequest(c.conn, utils.S2B(key))
	if err != nil {
		return err
	}

	b := make([]byte, 1024)
	_, err = c.conn.Read(b)
	if err != nil {
		return err
	}
	_, err = ParseOne(b)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Expire(key string, ttl int64) error {
	_, err := writeExpireRequest(c.conn, utils.S2B(key), ttl)
	if err != nil {
		return err
	}

	b := make([]byte, 1024)
	if err != nil {
		return err
	}

	_, err = ParseOne(b)
	if err != nil {
		return err
	}
	return nil
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

func (c *Client) Get(key string) (string, error) {
	_, err := writeGetRequest(c.conn, utils.S2B(key))
	if err != nil {
		return "", err
	}

	b := make([]byte, 1024)
	_, err = c.conn.Read(b)
	if err != nil {
		return "", err
	}

	reply, err := ParseOne(b)
	if err != nil {
		return "", err
	}

	ss := strings.Split(utils.B2S(reply.ToBytes()), CRLF)

	return ss[1], nil
}

func (c *Client) Set(key string, value string) error {
	_, err := writeSetRequest(c.conn, utils.S2B(key), utils.S2B(value))
	if err != nil {
		return err
	}

	b := make([]byte, 1024)
	_, err = c.conn.Read(b)
	if err != nil {
		return err
	}

	_, err = ParseOne(b)
	if err != nil {
		return err
	}
	return nil
}
