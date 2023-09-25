package rpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// Method 方法
type Method struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 第一个参数类型
	ReplyType reflect.Type   // 第二个参数类型
	numCalls  uint64
}

// NumCalls 获取调用次数
func (m *Method) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *Method) newArgv() reflect.Value {
	var argv reflect.Value

	if m.ArgType.Kind() == reflect.Ptr {
		// 创建指针类型
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// 创建值类型
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *Method) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

// 服务
type service struct {
	name    string        // 映射结构体名称
	typ     reflect.Type  // 结构体类型
	rcvr    reflect.Value // 结构体实例本身
	methods map[string]*Method
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
	svc.methods = make(map[string]*Method)

	// 遍历结构体中的方法
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

		// 添加映射到map
		svc.methods[method.Name] = &Method{
			method:    method,
			ArgType:   argt,
			ReplyType: rept,
		}
		log.Printf("rpc server: register %s.%s\n", svc.name, method.Name)
	}
}

func (svc *service) call(mt *Method, argv, repv reflect.Value) error {
	atomic.AddUint64(&mt.numCalls, 1)
	fn := mt.method.Func
	retvs := fn.Call([]reflect.Value{svc.rcvr, argv, repv})
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
