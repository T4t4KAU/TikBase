package tiko

import (
	"TikBase/iface"
	"TikBase/pack/utils"
	"errors"
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
	_, err := writeSetRequest(c.conn, utils.StringToBytes(key), utils.StringToBytes(value))
	if err != nil {
		return err
	}
	code, data, err := parseReply(c.conn)
	if err != nil {
		return err
	}
	if code != Success {
		return errors.New(utils.BytesToString(data))
	}
	return nil
}

func (c *Client) Get(key string) (string, error) {
	_, err := writeGetRequest(c.conn, utils.StringToBytes(key))
	if err != nil {
		return "", err
	}
	code, data, err := parseReply(c.conn)
	if err != nil {
		return "", err
	}
	if code != Success {
		return "", errors.New(utils.BytesToString(data))
	}
	return string(data), nil
}

func (c *Client) Del(key string) error {
	_, err := writeDelRequest(c.conn, utils.StringToBytes(key))
	if err != nil {
		return err
	}
	code, data, err := parseReply(c.conn)
	if err != nil {
		return err
	}
	if code != Success {
		return errors.New(utils.BytesToString(data))
	}
	return nil
}

func (c *Client) Expire(key string, ttl int64) error {
	_, err := writeExpireRequest(c.conn, utils.StringToBytes(key), utils.Int64ToBytes(ttl))
	if err != nil {
		return err
	}
	code, data, err := parseReply(c.conn)
	if err != nil {
		return err
	}
	if code != Success {
		return errors.New(string(data))
	}
	return nil
}
