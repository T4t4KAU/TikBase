package pack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/T4t4KAU/TikBase/iface"
)

const MaxPacketSize = 1024

type DataPack struct{}

func (pack *DataPack) GetHeadLen() uint32 {
	return 8
}

func (pack *DataPack) Pack(msg iface.IMessage) ([]byte, error) {
	b := bytes.NewBuffer([]byte{})

	if err := binary.Write(b, binary.LittleEndian, msg.GetDataLen()); err != nil {
		return nil, err
	}
	if err := binary.Write(b, binary.LittleEndian, msg.GetId()); err != nil {
		return nil, err
	}
	if err := binary.Write(b, binary.LittleEndian, msg.GetData()); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (pack *DataPack) Unpack(data []byte) (iface.IMessage, error) {
	b := bytes.NewReader(data)

	msg := &Message{}

	if err := binary.Read(b, binary.LittleEndian, &msg.DataLen); err != nil {
		return nil, err
	}

	if err := binary.Read(b, binary.LittleEndian, &msg.Id); err != nil {
		return nil, err
	}

	if msg.DataLen > MaxPacketSize {
		return nil, errors.New("too large pkg data received")
	}

	return msg, nil
}

func NewDataPack() *DataPack {
	return &DataPack{}
}
