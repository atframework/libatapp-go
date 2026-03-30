package module

import (
	"context"

	"github.com/atframework/libatapp-go/etcd_module/internal/codec"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"google.golang.org/protobuf/proto"
)

// SetMaybeUpdateKeepaliveTopologyArea 设置MaybeUpdateKeepaliveTopologyArea。
func (m *EtcdModule) SetMaybeUpdateKeepaliveTopologyArea() {}

// SetMaybeUpdateKeepaliveTopologyMetadata 设置MaybeUpdateKeepaliveTopologyMetadata。
func (m *EtcdModule) SetMaybeUpdateKeepaliveTopologyMetadata() {}

// SetTopologyKeepaliveData 设置TopologyKeepaliveData。
func (m *EtcdModule) SetTopologyKeepaliveData(info *pb.AtappTopologyInfo) {
	m.stageTopologyKeepalive(info)
}

// SetTopologyKeepaliveSource 设置TopologyKeepaliveSource。
func (m *EtcdModule) SetTopologyKeepaliveSource(source func() *pb.AtappTopologyInfo) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.topology.keepaliveSource = source
	m.mu.Unlock()
}

func (m *EtcdModule) stageTopologyKeepalive(info *pb.AtappTopologyInfo) {
	if m == nil || info == nil {
		return
	}
	m.mu.Lock()
	m.topology.pendingKeepalive = proto.Clone(info).(*pb.AtappTopologyInfo)
	m.topology.keepaliveDirty = true
	m.mu.Unlock()
}

func (m *EtcdModule) syncTopologyKeepaliveFromSource() {
	if m == nil {
		return
	}
	m.mu.RLock()
	source := m.topology.keepaliveSource
	dirty := m.topology.keepaliveDirty
	currentPending := m.topology.pendingKeepalive
	lastPublished := m.topology.lastKeepalive
	m.mu.RUnlock()
	if source == nil {
		return
	}

	latest := source()
	if latest == nil {
		return
	}
	latestClone := proto.Clone(latest).(*pb.AtappTopologyInfo)

	m.mu.Lock()
	defer m.mu.Unlock()

	if dirty {
		if currentPending == nil || !proto.Equal(currentPending, latestClone) {
			m.topology.pendingKeepalive = latestClone
		}
		return
	}

	if currentPending != nil && proto.Equal(currentPending, latestClone) {
		return
	}
	if lastPublished != nil && proto.Equal(lastPublished, latestClone) {
		return
	}

	m.topology.pendingKeepalive = latestClone
	m.topology.keepaliveDirty = true
}

func (m *EtcdModule) flushTopologyKeepalive(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.RLock()
	if !m.topology.keepaliveDirty || m.topology.pendingKeepalive == nil {
		m.mu.RUnlock()
		return nil
	}
	pending := proto.Clone(m.topology.pendingKeepalive).(*pb.AtappTopologyInfo)
	oldPath := m.topology.keepalivePath
	oldValue := m.topology.lastKeepalive
	m.mu.RUnlock()

	if pending.GetId() == 0 || pending.GetName() == "" {
		return nil
	}

	path := m.GetTopologyPath() + "/" + pending.GetName() + "-" + formatUint(pending.GetId())
	if oldPath == path && oldValue != nil && proto.Equal(oldValue, pending) {
		m.mu.Lock()
		m.topology.keepaliveDirty = false
		m.mu.Unlock()
		return nil
	}

	jsonValue, err := codec.MarshalProtoToJSON(pending)
	if err != nil {
		return err
	}

	cl := m.clusterCtx()
	if cl == nil || !cl.IsAvailable() {
		return nil
	}

	if oldPath != "" && oldPath != path {
		_ = cl.DeleteRawValue(ctx, oldPath)
	}

	if err := cl.PutRawValueWithClusterLease(ctx, path, string(jsonValue)); err != nil {
		return err
	}

	m.mu.Lock()
	m.topology.keepalivePath = path
	m.topology.lastKeepalive = proto.Clone(pending).(*pb.AtappTopologyInfo)
	m.topology.keepaliveDirty = false
	m.mu.Unlock()
	return nil
}
