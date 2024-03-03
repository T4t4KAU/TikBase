package slice

type Options struct {
	Name                 string
	Address              string
	ServerType           string
	VirtualNodeCount     int
	UpdateCircleDuration int
	cluster              []string
}

var DefaultOptions = Options{
	Address:              "127.0.0.1:8980",
	ServerType:           "tcp",
	VirtualNodeCount:     1024,
	UpdateCircleDuration: 3,
}
