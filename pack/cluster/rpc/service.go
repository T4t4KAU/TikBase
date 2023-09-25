package rpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 方法类型
type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

// NumCalls 获取调用次数
func (mt *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&mt.numCalls)
}

func (mt *methodType) newArgv() reflect.Value {
	var argv reflect.Value

	if mt.ArgType.Kind() == reflect.Ptr {
		// 创建指针类型
		argv = reflect.New(mt.ArgType.Elem())
	} else {
		// 创建值类型
		argv = reflect.New(mt.ArgType).Elem()
	}
	return argv
}

func (mt *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(mt.ReplyType.Elem())
	switch mt.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(mt.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(mt.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name    string        // 映射结构体名称
	typ     reflect.Type  // 结构体类型
	rcvr    reflect.Value // 结构体实例本身
	methods map[string]*methodType
}

// 构造函数
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// 过滤符合条件的方法
func (svc *service) registerMethods() {
	svc.methods = make(map[string]*methodType)
	for i := 0; i < svc.typ.NumMethod(); i++ {
		method := svc.typ.Method(i)
		mt := method.Type // 类型
		if mt.NumIn() != 3 || mt.NumOut() != 1 {
			continue
		}
		if mt.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argt, rept := mt.In(1), mt.In(2)
		if !isExportedOrBuiltinType(argt) || !isExportedOrBuiltinType(rept) {
			continue
		}
		svc.methods[method.Name] = &methodType{
			method:    method,
			ArgType:   argt,
			ReplyType: rept,
		}
		log.Printf("rpc server: register %s.%s\n", svc.name, method.Name)
	}
}

func (svc *service) call(mt *methodType, argv, repv reflect.Value) error {
	atomic.AddUint64(&mt.numCalls, 1)
	f := mt.method.Func
	retvs := f.Call([]reflect.Value{svc.rcvr, argv, repv})
	err := retvs[0].Interface()
	if err != nil {
		return err.(error)
	}
	return nil
}

// 判断是否为导出字段
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
