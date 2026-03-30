package module

import (
	"context"

	"github.com/atframework/libatapp-go/etcd_module/events"
	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/internal/topology"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"google.golang.org/protobuf/proto"
)

// TopologyInfo 定义TopologyInfo类型。
type TopologyInfo = topology.Info

// TopologyWatcherSender 定义TopologyWatcherSender类型。
type TopologyWatcherSender struct {
	AtappModule *EtcdModule
	EtcdHeader  *pb.EtcdResponseHeader
	EtcdBody    []*watcher.EtcdWatchEvent
	Event       *watcher.EtcdWatchEvent
	Topology    *TopologyInfo
	Action      TopologyAction
}

// TopologyWatcherListCallback 定义TopologyWatcherListCallback回调函数类型。
type TopologyWatcherListCallback func(sender *TopologyWatcherSender)

// TopologyCompatStorage 定义TopologyCompatStorage类型。
type TopologyCompatStorage = topology.CompatStorage

// TopologyCompatEvent 定义TopologyCompatEvent类型。
type TopologyCompatEvent struct {
	EtcdHeader *pb.EtcdResponseHeader
	Storage    *TopologyCompatStorage
	Action     TopologyAction
}

const topologySenderMetadataKey = "topology_sender"

func (t *etcdModuleTopology) setLastHeader(header *pb.EtcdResponseHeader) {
	if t == nil || header == nil {
		return
	}
	t.lastHeader = proto.Clone(header).(*pb.EtcdResponseHeader)
}

func (t *etcdModuleTopology) getLastHeaderClone() *pb.EtcdResponseHeader {
	if t == nil || t.lastHeader == nil {
		return nil
	}
	return proto.Clone(t.lastHeader).(*pb.EtcdResponseHeader)
}

func (t *etcdModuleTopology) setSnapshotReady(ready bool) {
	if t == nil {
		return
	}
	t.snapshotReady = ready
}

func (t *etcdModuleTopology) isSnapshotReady() bool {
	if t == nil {
		return false
	}
	return t.snapshotReady
}

func (t *etcdModuleTopology) ensureOnLoadSnapshotCallbacks() *events.CallbackList {
	if t == nil {
		return nil
	}
	if t.onLoadSnapshot == nil {
		t.onLoadSnapshot = events.NewCallbackList()
	}
	return t.onLoadSnapshot
}

func (t *etcdModuleTopology) ensureOnSnapshotLoadedCallbacks() *events.CallbackList {
	if t == nil {
		return nil
	}
	if t.onSnapshotLoaded == nil {
		t.onSnapshotLoaded = events.NewCallbackList()
	}
	return t.onSnapshotLoaded
}

func (t *etcdModuleTopology) getOnLoadSnapshotCallbacks() *events.CallbackList {
	if t == nil {
		return nil
	}
	return t.onLoadSnapshot
}

func (t *etcdModuleTopology) getOnSnapshotLoadedCallbacks() *events.CallbackList {
	if t == nil {
		return nil
	}
	return t.onSnapshotLoaded
}

func (t *etcdModuleTopology) ensureOnInfoEventCallbacks() *events.CallbackList {
	if t == nil {
		return nil
	}
	if t.onInfoEvent == nil {
		t.onInfoEvent = events.NewCallbackList()
	}
	return t.onInfoEvent
}

func (t *etcdModuleTopology) getOnInfoEventCallbacks() *events.CallbackList {
	if t == nil {
		return nil
	}
	return t.onInfoEvent
}

func (t *etcdModuleTopology) sortedInfoSet() []*TopologyInfo {
	if t == nil {
		return nil
	}
	return topology.SortedInfos(t.infoSet)
}

func (t *etcdModuleTopology) deleteInfo(path string) {
	if t == nil {
		return
	}
	delete(t.infoSet, path)
}

func (t *etcdModuleTopology) upsertInfo(info *TopologyInfo) {
	if t == nil || info == nil {
		return
	}
	t.infoSet[info.Path] = &TopologyInfo{
		Path:        info.Path,
		Revision:    info.Revision,
		DataVersion: etcdversion.New(info.CreateRevision, info.ModRevision, info.Version),
		Value:       topology.CloneMap(info.Value),
	}
}

func (t *etcdModuleTopology) resetInfoSet() {
	if t == nil {
		return
	}
	t.infoSet = map[string]*TopologyInfo{}
}

// GetTopologyPath 获取TopologyPath。
func (m *EtcdModule) GetTopologyPath() string {
	return m.GetConfigurePath() + "/" + topology.DirectoryName
}

// GetTopologyWatcherPath 获取TopologyWatcherPath。
func (m *EtcdModule) GetTopologyWatcherPath() string {
	return m.GetTopologyPath()
}

// AddTopologyWatcherCallback 添加TopologyWatcherCallback。
func (m *EtcdModule) AddTopologyWatcherCallback(ctx context.Context, fn TopologyWatcherListCallback) error {
	w, err := m.AddWatcherByCustomPathAndGet(ctx, m.GetTopologyWatcherPath())
	if err != nil {
		return err
	}

	w.SetSnapshotLoadingHandler(func() {
		m.mu.Lock()
		m.topology.setSnapshotReady(false)
		m.topology.resetInfoSet()
		m.mu.Unlock()
		m.dispatchOnLoadTopologySnapshot()
	})
	w.SetSnapshotLoadedHandler(func(nodes []*watcher.EtcdWatchEvent) {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			sender := m.buildTopologyWatcherSender(*node)
			m.applyTopologyEvent(sender)
		}
		m.mu.Lock()
		m.topology.setSnapshotReady(true)
		m.mu.Unlock()
		m.dispatchOnTopologySnapshotLoaded()
	})

	if fn != nil {
		w.AddHandler(func(event watcher.EtcdWatchEvent) {
			sender := m.buildTopologyWatcherSender(event)
			m.applyTopologyEvent(sender)
			fn(sender)
		})
	}
	return nil
}

// AddTopologyWatcher 添加TopologyWatcher。
func (m *EtcdModule) AddTopologyWatcher(ctx context.Context, fn TopologyWatcherListCallback) error {
	return m.AddTopologyWatcherCallback(ctx, fn)
}

// GetLastEtcdEventTopologyHeader 获取LastEtcdEventTopologyHeader。
func (m *EtcdModule) GetLastEtcdEventTopologyHeader() *pb.EtcdResponseHeader {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.topology.getLastHeaderClone()
}

// HasTopologySnapshot 判断是否存在TopologySnapshot。
func (m *EtcdModule) HasTopologySnapshot() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.topology.isSnapshotReady()
}

// AddOnLoadTopologySnapshot 添加OnLoadTopologySnapshot。
func (m *EtcdModule) AddOnLoadTopologySnapshot(fn SnapshotEventCallback) events.EventCallbackHandle {
	if m == nil || fn == nil {
		return 0
	}
	m.mu.Lock()
	callbacks := m.topology.ensureOnLoadSnapshotCallbacks()
	m.mu.Unlock()

	return callbacks.Add(func(event *events.Event) {
		_ = event
		fn(m)
	})
}

// RemoveOnLoadTopologySnapshot 移除OnLoadTopologySnapshot。
func (m *EtcdModule) RemoveOnLoadTopologySnapshot(handle events.EventCallbackHandle) {
	if m == nil {
		return
	}
	m.mu.RLock()
	callbacks := m.topology.getOnLoadSnapshotCallbacks()
	m.mu.RUnlock()
	if callbacks == nil {
		return
	}
	callbacks.Remove(handle)
}

// AddOnTopologySnapshotLoaded 添加OnTopologySnapshotLoaded。
func (m *EtcdModule) AddOnTopologySnapshotLoaded(fn SnapshotEventCallback) events.EventCallbackHandle {
	if m == nil || fn == nil {
		return 0
	}
	m.mu.Lock()
	callbacks := m.topology.ensureOnSnapshotLoadedCallbacks()
	m.mu.Unlock()

	return callbacks.Add(func(event *events.Event) {
		_ = event
		fn(m)
	})
}

// RemoveOnTopologySnapshotLoaded 移除OnTopologySnapshotLoaded。
func (m *EtcdModule) RemoveOnTopologySnapshotLoaded(handle events.EventCallbackHandle) {
	if m == nil {
		return
	}
	m.mu.RLock()
	callbacks := m.topology.getOnSnapshotLoadedCallbacks()
	m.mu.RUnlock()
	if callbacks == nil {
		return
	}
	callbacks.Remove(handle)
}

// AddOnTopologyInfoEvent 添加OnTopologyInfoEvent。
func (m *EtcdModule) AddOnTopologyInfoEvent(fn TopologyWatcherListCallback) events.EventCallbackHandle {
	if m == nil || fn == nil {
		return 0
	}
	m.mu.Lock()
	callbacks := m.topology.ensureOnInfoEventCallbacks()
	m.mu.Unlock()

	return callbacks.Add(func(event *events.Event) {
		if event == nil || event.Metadata == nil {
			return
		}
		rawSender, ok := event.Metadata[topologySenderMetadataKey]
		if !ok {
			return
		}
		sender, ok := rawSender.(*TopologyWatcherSender)
		if !ok || sender == nil {
			return
		}
		fn(sender)
	})
}

// RemoveOnTopologyInfoEvent 移除OnTopologyInfoEvent。
func (m *EtcdModule) RemoveOnTopologyInfoEvent(handle events.EventCallbackHandle) {
	if m == nil {
		return
	}
	m.mu.RLock()
	callbacks := m.topology.getOnInfoEventCallbacks()
	m.mu.RUnlock()
	if callbacks == nil {
		return
	}
	callbacks.Remove(handle)
}

// GetTopologyInfoSet 获取TopologyInfoSet。
func (m *EtcdModule) GetTopologyInfoSet() []*TopologyInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.topology.sortedInfoSet()
}

// GetTopologyInfoSetCompat 获取TopologyInfoSetCompat。
func (m *EtcdModule) GetTopologyInfoSetCompat() map[uint64]*TopologyCompatStorage {
	ret := map[uint64]*TopologyCompatStorage{}
	if m == nil {
		return ret
	}
	for _, item := range m.GetTopologyInfoSet() {
		storage := topology.BuildCompatStorage(item)
		if storage == nil || storage.Info == nil || storage.Info.GetId() == 0 {
			continue
		}
		ret[storage.Info.GetId()] = storage
	}
	return ret
}

// TopologyValueToProto 将 TopologyValueToProto 转换为 Proto 数据。
func TopologyValueToProto(raw map[string]any) *pb.AtappTopologyInfo {
	return topology.ValueToProto(raw)
}

// BuildTopologyCompatEvent 构建TopologyCompatEvent。
func BuildTopologyCompatEvent(sender *TopologyWatcherSender) *TopologyCompatEvent {
	if sender == nil {
		return nil
	}
	return &TopologyCompatEvent{
		EtcdHeader: sender.EtcdHeader,
		Storage:    topology.BuildCompatStorage(sender.Topology),
		Action:     sender.Action,
	}
}
