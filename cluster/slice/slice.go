package slice

import (
	"github.com/T4t4KAU/TikBase/cluster/chash"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/hashicorp/memberlist"
	"io/ioutil"
)

type Slice struct {
	options     Options
	address     string
	circle      *chash.ConsistentHash
	nodeManager *memberlist.Memberlist
	iface.Engine
}

func New(options Options) (*Slice, error) {
	if options.cluster == nil || len(options.cluster) == 0 {
		options.cluster = []string{options.Address}
	}

	manager, err := createNodeManager(options)
	if err != nil {
		return nil, err
	}

	slice := &Slice{
		options:     options,
		address:     options.Address,
		circle:      chash.New(options.VirtualNodeCount, chash.DefaultHash),
		nodeManager: manager,
	}

	return slice, nil
}

func createNodeManager(options Options) (*memberlist.Memberlist, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = options.Address
	config.BindAddr, _ = utils.SplitAddressAndPort(options.Address)
	config.LogOutput = ioutil.Discard

	manager, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	_, err = manager.Join(options.cluster)
	return manager, err
}

func (s *Slice) SelectNode(key string) (string, error) {
	return s.circle.GetNode(key)
}

func (s *Slice) IsCurrentNode(address string) bool {
	return s.address == address
}
