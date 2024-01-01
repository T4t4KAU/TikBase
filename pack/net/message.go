package net

type Message struct {
	Id      uint32
	DataLen uint32
	Data    []byte
}

func (msg *Message) GetDataLen() uint32 {
	//TODO implement me
	panic("implement me")
}

func (msg *Message) GetId() uint32 {
	//TODO implement me
	panic("implement me")
}

func (msg *Message) GetData() []byte {
	//TODO implement me
	panic("implement me")
}

func (msg *Message) SetMsgId() uint32 {
	//TODO implement me
	panic("implement me")
}

func (msg *Message) SetData() []byte {
	//TODO implement me
	panic("implement me")
}

func (msg *Message) SetDataLen(u uint32) {
	//TODO implement me
	panic("implement me")
}

func NewMsgPackage(id uint32, data []byte) *Message {
	return &Message{
		Id:      id,
		DataLen: uint32(len(data)),
		Data:    data,
	}
}
