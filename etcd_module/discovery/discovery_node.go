package discovery

import (
	"sync"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"google.golang.org/protobuf/proto"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// OnDestroyFunc 定义OnDestroyFunc回调函数类型。
type OnDestroyFunc func(node *DiscoveryNode)

// DiscoveryNode 定义DiscoveryNode类型。
type DiscoveryNode struct {
	Info *pb.AtappDiscovery
	Path string // The full path/key in etcd

	// Causion: modify_revision of multiple key may be same when they are modifiedx in one transaction of etcd
	// @see https://etcd.io/docs/v3.5/learning/api_guarantees/#revision
	etcdversion.DataVersion

	IngressIndex int

	IngressForListen *pb.AtappGateway

	nameHash [2]uint64 // cached SHA256 hash of Info.Name; nameHash[0]=lo, nameHash[1]=hi

	// privateData 是透传给上层的私有数据槽位（Go 原生做法）。
	// 为保持与 C++ union 语义一致，同一时刻仅保留最近一次写入的一个值。
	privateData any

	mu        sync.Mutex
	onDestroy OnDestroyFunc
}

func (n *DiscoveryNode) GetIngressSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Info == nil {
		return 0
	}

	if len(n.Info.GetGateways()) > 0 {
		return len(n.Info.GetGateways())
	}

	return len(n.Info.GetListen())
}

func (n *DiscoveryNode) NextIngressGateway() *pb.AtappGateway {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Info == nil {
		return nil
	}

	if n.IngressIndex < 0 {
		n.IngressIndex = 0
	}

	if len(n.Info.GetGateways()) > 0 {
		if n.IngressIndex >= len(n.Info.GetGateways()) {
			n.IngressIndex %= len(n.Info.GetGateways())
		}
		return n.Info.GetGateways()[n.IngressIndex]
	}

	if len(n.Info.GetListen()) > 0 {
		if n.IngressIndex >= len(n.Info.GetListen()) {
			n.IngressIndex %= len(n.Info.GetListen())
		}
		if n.IngressForListen == nil {
			n.IngressForListen = &pb.AtappGateway{}
		}
		n.IngressForListen.Address = n.Info.GetListen()[n.IngressIndex]
		n.IngressIndex++
		return n.IngressForListen
	}

	// if none of gateways or listen found, ingress_for_listen_ will always be empty
	return n.IngressForListen
}

func (n *DiscoveryNode) UpdateVersion(createRevision int64, modRevision int64, version int64, upgrade bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if upgrade {
		if createRevision > n.CreateRevision {
			n.CreateRevision = createRevision
			n.Version = version
		}

		if modRevision > n.ModRevision {
			n.ModRevision = modRevision
			n.Version = version
		}

		if version > n.Version {
			n.Version = version
		}
	} else {
		n.Version = version
	}
}

func (n *DiscoveryNode) CopyFrom(info *pb.AtappDiscovery, createRevision, modRevision, version int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if info != nil {
		n.Info = proto.Clone(info).(*pb.AtappDiscovery)
	} else {
		n.Info = nil
	}

	// Recompute name hash (equivalent to C++ consistent_hash_calc on name)
	if info != nil && info.Name != "" {
		n.nameHash[0], n.nameHash[1] = consistentHashKey([]byte(info.Name), consistentHashMagicSeed)
	} else {
		n.nameHash[0] = 0
		n.nameHash[1] = 0
	}

	n.CreateRevision = createRevision
	n.ModRevision = modRevision
	n.Version = version
}

func (n *DiscoveryNode) CopyTo(output *pb.AtappDiscovery) {
	if output == nil {
		return
	}
	n.mu.Lock()
	var snapshot *pb.AtappDiscovery
	if n.Info != nil {
		snapshot = proto.Clone(n.Info).(*pb.AtappDiscovery)
	}
	n.mu.Unlock()

	proto.Reset(output)
	if snapshot != nil {
		proto.Merge(output, snapshot)
	}
}

func (n *DiscoveryNode) CopyKeyTo(output *pb.AtappDiscovery) {
	if output == nil {
		return
	}
	n.mu.Lock()
	var src *pb.AtappDiscovery
	if n.Info != nil {
		src = proto.Clone(n.Info).(*pb.AtappDiscovery)
	}
	n.mu.Unlock()
	if src == nil {
		return
	}
	output.Id = src.Id
	output.Name = src.Name
	output.Identity = src.Identity
	output.HashCode = src.HashCode
	output.TypeName = src.TypeName
	output.TypeId = src.TypeId
	output.Pid = src.Pid
	output.Hostname = src.Hostname
}

func (n *DiscoveryNode) SetOnDestroy(fn OnDestroyFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onDestroy = fn
}

func (n *DiscoveryNode) GetOnDestroy() OnDestroyFunc {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.onDestroy
}

func (n *DiscoveryNode) ResetOnDestroy() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onDestroy = nil
}

// SetPrivateData 设置透传私有数据（Go Native 通用入口）。
func (n *DiscoveryNode) SetPrivateData(v any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.privateData = v
}

// GetPrivateData 获取透传私有数据（Go Native 通用入口）。
func (n *DiscoveryNode) GetPrivateData() any {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.privateData
}

// GetNameHash 获取NameHash。
func (n *DiscoveryNode) GetNameHash() (uint64, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nameHash[0], n.nameHash[1]
}

// WithDiscoveryInfo 在持有读锁的情况下执行回调函数访问 DiscoveryInfo。
// 这是替代直接指针返回的安全模式，确保调用者无法在锁外修改返回的指针。
func (n *DiscoveryNode) WithDiscoveryInfo(fn func(*pb.AtappDiscovery)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if fn != nil {
		fn(n.Info)
	}
}

// GetNodeVersion 获取NodeVersion。
func (n *DiscoveryNode) GetNodeVersion() (createRevision, modRevision, version int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.CreateRevision, n.ModRevision, n.Version
}

// Equal 按节点身份比较两个节点是否等价（与 C++ node_equal 语义一致）。
// 当双方 ID 都非 0 时按 ID 比较，否则按 Name 比较。
func (n *DiscoveryNode) Equal(other *DiscoveryNode) bool {
	if n == other {
		return true
	}

	if other == nil {
		return false
	}

	var left, right *pb.AtappDiscovery
	n.WithDiscoveryInfo(func(info *pb.AtappDiscovery) {
		left = info
	})
	other.WithDiscoveryInfo(func(info *pb.AtappDiscovery) {
		right = info
	})

	if left.GetId() != 0 && right.GetId() != 0 {
		return left.GetId() == right.GetId()
	}

	return left.GetName() == right.GetName()
}
