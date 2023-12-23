package resp

import (
	"errors"
	"github.com/T4t4KAU/TikBase/pack/utils"
	"net"
	"strings"
)

type Client struct {
	conn net.Conn
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

	s := string(b)
	if s[0] == '-' {
		return "", errors.New(s[1:])
	}

	ss := strings.Split(s, CRLF)

	return ss[1], nil
}

func (c *Client) Set(key string, value string) (string, error) {
	_, err := writeSetRequest(c.conn, utils.S2B(key), utils.S2B(value))
	if err != nil {
		return "", err
	}

	payloads := readLine(c.conn)
	return payloads[0].Data.(*StatusReply).Status, nil
}
