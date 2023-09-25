package codec

import (
	"bufio"
	"encoding/gob"
	"io"
)

var _ Codec = (*GobCodec)(nil)

type GobCodec struct {
	conn    io.ReadWriteCloser
	buff    *bufio.Writer
	decoder *gob.Decoder
	encoder *gob.Encoder
}

// NewGobCodec 创建Gob编解码器
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buff := bufio.NewWriter(conn)
	return &GobCodec{
		conn:    conn,
		buff:    buff,
		decoder: gob.NewDecoder(conn),
		encoder: gob.NewEncoder(conn),
	}
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

// ReadHeader 读取消息头
func (c *GobCodec) ReadHeader(header *Header) error {
	return c.decoder.Decode(header)
}

// ReadBody 读取消息体
func (c *GobCodec) ReadBody(body any) error {
	return c.decoder.Decode(body)
}

// 写入消息
func (c *GobCodec) Write(header *Header, body any) error {
	var err error

	defer func() {
		c.buff.Flush()
		if err != nil {
			c.Close()
		}
	}()

	// 编码数据后写入
	err = c.encoder.Encode(header)
	if err != nil {
		return err
	}
	err = c.encoder.Encode(body)
	if err != nil {
		return err
	}
	return nil
}
