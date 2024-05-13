package slice

import (
	"github.com/T4t4KAU/TikBase/iface"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/hashicorp/memberlist"
	"io/ioutil"
	"stathat.com/c/consistent"
	"time"
)

// Slice 数据切片
type Slice struct {
	options      Options                // 配置信息
	address      string                 // 地址
	circle       *consistent.Consistent // 一致性哈希
	nodeManager  *memberlist.Memberlist // 节点管理器
	iface.Engine                        // 存储引擎
}

// New 创建并启动数据切片
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
		circle:      consistent.New(),
		nodeManager: manager,
		Engine:      eng,
	}

	slice.circle.NumberOfReplicas = options.VirtualNodeCount
	slice.autoUpdateCircle()

	return slice, nil
}

// 创建节点管理器
func createNodeManager(options Options) (*memberlist.Memberlist, error) {
	config := memberlist.DefaultLANConfig() // 在默认LAN配置上进行配置
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
	return s.circle.Get(key)
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

// 更新哈希环
func (s *Slice) updateCircle() {
	s.circle.Set(s.Nodes())
}

// 自动更新哈希环
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
