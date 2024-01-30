package rpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 远程调用方法
type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

// NumCalls 调用次数
func (mt *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&mt.numCalls)
}

func (mt *methodType) newArgValue() reflect.Value {
	var argv reflect.Value
	// 判断是否为指针类型
	if mt.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mt.ArgType.Elem())
	} else {
		argv = reflect.New(mt.ArgType).Elem()
	}
	return argv
}

func (mt *methodType) newReplyValue() reflect.Value {
	replyv := reflect.New(mt.ReplyType.Elem())
	switch mt.ReplyType.Elem().Kind() {
	case reflect.Map: // 处理map类型
		replyv.Elem().Set(reflect.MakeMap(mt.ReplyType.Elem()))
	case reflect.Slice: // 处理切片类型
		replyv.Elem().Set(reflect.MakeSlice(mt.ReplyType.Elem(), 0, 0))
	default:
	}
	return replyv
}

type service struct {
	name   string // 结构体名称
	typ    reflect.Type
	rcvr   reflect.Value          // 指向结构体自身
	method map[string]*methodType // 远程方法
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)

	// 结构体是否导出
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid region name", s.name)
	}

	// 注册所有方法
	s.registerMethods()
	return s
}

func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type

		// 检查入参是否为3 出参是否为1
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}

		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}

		// 注册远程方法
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	fn := m.method.Func
	returnValues := fn.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
