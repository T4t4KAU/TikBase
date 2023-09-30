package engine

import (
	"TikBase/pack/iface"
)

type EchoEngine struct{}

type EchoResult struct {
	success bool
	data    [][]byte
}

func (r *EchoResult) Success() bool {
	return r.success
}

func (r *EchoResult) Error() error {
	return nil
}

func (r *EchoResult) Status() int {
	return 0
}

func (r *EchoResult) Data() [][]byte {
	return r.data
}

func NewResult(succ bool, data [][]byte) *EchoResult {
	return &EchoResult{
		success: succ,
		data:    data,
	}
}

func NewEchoEngine() *EchoEngine {
	return &EchoEngine{}
}

func (eng *EchoEngine) Exec(ins iface.INS, args [][]byte) iface.Result {
	if ins != iface.ECHO {
		return NewResult(false, nil)
	}
	return NewResult(true, args)
}

func NewSuccResult() EchoResult {
	return EchoResult{
		success: true,
	}
}
