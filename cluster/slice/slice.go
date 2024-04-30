package slice

import (
	"github.com/T4t4KAU/TikBase/cluster/chash"
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/hashicorp/memberlist"
	"io/ioutil"
	"time"
)

// Slice 数据切片
type Slice struct {
	options      Options                // 配置信息
	address      string                 // 地址
	circle       *chash.ConsistentHash  // 一致性哈希
	nodeManager  *memberlist.Memberlist // 节点管理器
	iface.Engine                        // 存储引擎
}

func New(options Options, eng iface.Engine) (*Slice, error) {
	if options.Cluster == nil || len(options.Cluster) == 0 {
		options.Cluster = []string{options.Address}
	}

	// 创建节点管理器
	manager, err := createNodeManager(options)
	if err != nil {
		return nil, err
	}

	slice := &Slice{
		options:     options,
		address:     options.Address,
		circle:      chash.New(options.VirtualNodeCount, chash.DefaultHash),
		nodeManager: manager,
		Engine:      eng,
	}

	// 添加新节点
	slice.circle.AddNode(options.Address)

	return slice, nil
}

// 创建节点管理器
func createNodeManager(options Options) (*memberlist.Memberlist, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = options.Name

	config.BindAddr, config.BindPort, _ = utils.SplitAddressAndPort(options.Address)
	config.LogOutput = ioutil.Discard // 禁用日志输出

	// 创建管理器
	manager, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	// 加入到指定的集群
	_, err = manager.Join(options.Cluster)
	return manager, err
}

// SelectNode 选择节点
func (s *Slice) SelectNode(key string) (string, error) {
	return s.circle.GetNode(key)
}

func (s *Slice) IsCurrentNode(address string) bool {
	return s.address == address
}

func (s *Slice) Nodes() []string {
	members := s.nodeManager.Members() // 获取成员
	nodes := make([]string, len(members))
	for i, member := range members {
		nodes[i] = member.Name
	}

	return nodes
}

func (s *Slice) updateCircle() {
	s.circle.AddNode(s.Nodes()...)
}

func (s *Slice) autoUpdateCircle() {
	s.updateCircle()

	go func() {
		duration := time.Duration(s.options.UpdateCircleDuration) * time.Second
		ticker := time.NewTicker(duration)

		for {
			select {
			case <-ticker.C:
				s.updateCircle()
			}
		}
	}()
}
