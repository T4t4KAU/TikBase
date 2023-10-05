package tiko

import (
	"TikBase/iface"
)

type Client struct {
	conn iface.Connection
}

func NewClient(conn iface.Connection) *Client {
	return &Client{
		conn: conn,
	}
}

func (c *Client) Set(key string, value string) error {
	_, err := writeSetRequest(c.conn, []byte(key), []byte(value))
	if err != nil {
		return err
	}
	code, _, err := parseReply(c.conn)
	if code != Success {
		return errExecuteCommand
	}
	return nil
}

func (c *Client) Get(key string) (string, error) {
	_, err := writeGetRequest(c.conn, []byte(key))
	if err != nil {
		return "", err
	}
	_, data, err := parseReply(c.conn)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
