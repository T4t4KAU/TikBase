package rpc

import (
	"fmt"
	"reflect"
	"testing"
)

type Foo int

type Args struct {
	Num1 int
	Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func _assert(condition bool, msg string, v ...any) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func TestNewService(t *testing.T) {
	var foo Foo

	s := newService(&foo)
	mt := s.methods["Sum"]

	argv := mt.newArgv()
	repv := mt.newReplyv()

	argv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 3}))
	err := s.call(mt, argv, repv)
	_assert(err == nil && *repv.Interface().(*int) == 4 && mt.NumCalls() == 1, "failed to call Foo.Sum")
}
