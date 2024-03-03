package slice

type Options struct {
	Name                 string
	Address              string
	ServerType           string
	VirtualNodeCount     int
	UpdateCircleDuration int
	Cluster              []string
}

var DefaultOptions = Options{
	Address:              "127.0.0.1",
	ServerType:           "tcp",
	VirtualNodeCount:     1024,
	UpdateCircleDuration: 3,
}
