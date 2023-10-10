package cluster

type Peer interface {
	Pick(key string)
}
