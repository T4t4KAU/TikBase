package proxy

type Proxy struct {
	limiter *Limiter
}

func New() *Proxy {
	return &Proxy{}
}

func (p *Proxy) Start() {

}
