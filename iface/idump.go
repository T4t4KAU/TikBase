package iface

type Dump interface {
	SaveTo(path string) error
	LoadFrom(path string) (KVStore, error)
}
