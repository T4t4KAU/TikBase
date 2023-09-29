package iface

type Dump interface {
	To(path string)
	From(path string)
}
