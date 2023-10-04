package iface

type Channel interface {
	Write(p []byte) (n int, err error)
	Close()
	Consume()
	Available() bool
}
