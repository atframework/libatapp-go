package libatapp

import (
	"context"
	"log/slog"

	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	moduleimpl "github.com/atframework/libatapp-go/etcd_module/module"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

var _ EtcdAppModuleImpl = (*etcdModuleAdapter)(nil)

type etcdModuleAdapter struct {
	AppModuleBase

	impl *moduleimpl.EtcdModule
}

func newEtcdModuleAdapter(owner AppImpl) *etcdModuleAdapter {
	return &etcdModuleAdapter{
		AppModuleBase: CreateAppModuleBase(owner),
	}
}

func (m *etcdModuleAdapter) ensureImpl() error {
	if m.impl != nil {
		return nil
	}
	owner := m.GetApp()
	if owner == nil {
		return nil
	}
	cfg := owner.GetConfig()
	if cfg == nil || cfg.ConfigPb == nil || cfg.ConfigPb.GetEtcd() == nil {
		return nil
	}
	mod, err := moduleimpl.NewEtcdModuleFromConfig(cfg.ConfigPb.GetEtcd(), slog.Default())
	if err != nil {
		return err
	}
	mod.SetTopologyKeepaliveSource(func() *pb.AtappTopologyInfo {
		owner := m.GetApp()
		if owner == nil {
			return nil
		}
		var cfgPB *pb.AtappConfigure
		if cfg := owner.GetConfig(); cfg != nil {
			cfgPB = cfg.ConfigPb
		}
		return moduleimpl.BuildTopologyKeepaliveInfoFromFields(
			owner.GetId(),
			owner.GetAppName(),
			owner.GetAppIdentity(),
			owner.GetHashCode(),
			owner.GetAppVersion(),
			owner.GetBuildVersion(),
			cfgPB,
		)
	})
	m.impl = mod
	return nil
}

func (m *etcdModuleAdapter) Name() string {
	return "etcd_module"
}

func (m *etcdModuleAdapter) Setup(parent context.Context) error {
	_ = parent
	return m.ensureImpl()
}

func (m *etcdModuleAdapter) Init(parent context.Context) error {
	if err := m.ensureImpl(); err != nil {
		return err
	}
	if m.impl == nil {
		return nil
	}
	return m.impl.Init(parent)
}

func (m *etcdModuleAdapter) Reload() error {
	if err := m.ensureImpl(); err != nil {
		return err
	}
	if m.impl == nil {
		return nil
	}
	owner := m.GetApp()
	if owner == nil {
		return nil
	}
	cfg := owner.GetConfig()
	if cfg == nil || cfg.ConfigPb == nil {
		return nil
	}
	return m.impl.Reload(context.Background(), cfg.ConfigPb.GetEtcd())
}

func (m *etcdModuleAdapter) Stop() (bool, error) {
	if m.impl == nil {
		return true, nil
	}
	return true, m.impl.Stop(context.Background())
}

func (m *etcdModuleAdapter) Cleanup() {
	if m.impl == nil {
		return
	}
	_ = m.impl.Close(context.Background())
}

func (m *etcdModuleAdapter) Timeout() {}

func (m *etcdModuleAdapter) Tick(parent context.Context) bool {
	if m.impl == nil {
		return false
	}
	_ = m.impl.Tick(parent)
	return false
}

func (m *etcdModuleAdapter) Reset() {
	if m.impl == nil {
		return
	}
	_ = m.impl.Reset(context.Background())
}

func (m *etcdModuleAdapter) GetConfCustomData() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetConfCustomData()
}

func (m *etcdModuleAdapter) SetConfCustomData(v string) {
	if m.impl == nil {
		return
	}
	m.impl.SetConfCustomData(v)
}

func (m *etcdModuleAdapter) GetConfigure() *pb.AtappEtcd {
	if m.impl == nil {
		return nil
	}
	return m.impl.GetConfigure()
}

func (m *etcdModuleAdapter) GetConfigurePath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetConfigurePath()
}

func (m *etcdModuleAdapter) IsEtcdEnabled() bool {
	if m.impl == nil {
		return false
	}
	return m.impl.IsEtcdEnabled()
}

func (m *etcdModuleAdapter) EnableEtcd() {
	if m.impl == nil {
		return
	}
	m.impl.EnableEtcd()
}

func (m *etcdModuleAdapter) DisableEtcd() {
	if m.impl == nil {
		return
	}
	m.impl.DisableEtcd()
}

func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveTopologyValue() {
	if m.impl == nil {
		return
	}
	m.impl.SetMaybeUpdateKeepaliveTopologyValue()
}

func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveDiscoveryValue() {
	if m.impl == nil {
		return
	}
	m.impl.SetMaybeUpdateKeepaliveDiscoveryValue()
}

func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveDiscoveryArea() {
	if m.impl == nil {
		return
	}
	m.impl.SetMaybeUpdateKeepaliveDiscoveryArea()
}

func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveDiscoveryMetadata() {
	if m.impl == nil {
		return
	}
	m.impl.SetMaybeUpdateKeepaliveDiscoveryMetadata()
}

func (m *etcdModuleAdapter) GetDiscoveryByIdPath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetDiscoveryByIDWatcherPath()
}

func (m *etcdModuleAdapter) GetDiscoveryByNamePath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetDiscoveryByNameWatcherPath()
}

func (m *etcdModuleAdapter) GetTopologyPath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetTopologyPath()
}

func (m *etcdModuleAdapter) GetDiscoveryByIdWatcherPath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetDiscoveryByIDWatcherPath()
}

func (m *etcdModuleAdapter) GetDiscoveryByNameWatcherPath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetDiscoveryByNameWatcherPath()
}

func (m *etcdModuleAdapter) GetTopologyWatcherPath() string {
	if m.impl == nil {
		return ""
	}
	return m.impl.GetTopologyWatcherPath()
}

func (m *etcdModuleAdapter) AddDiscoveryWatcherById(fn DiscoveryWatcherListCallback) error {
	if m.impl == nil {
		return nil
	}
	return m.impl.AddWatcherByIDCallback(context.Background(), func(sender *moduleimpl.WatcherSenderList) {
		if fn == nil || sender == nil {
			return
		}
		fn(&DiscoveryWatcherSender{
			Module:     m,
			EtcdHeader: sender.EtcdHeader,
			Node:       toNodeInfo(sender),
		})
	})
}

func (m *etcdModuleAdapter) AddDiscoveryWatcherByName(fn DiscoveryWatcherListCallback) error {
	if m.impl == nil {
		return nil
	}
	return m.impl.AddWatcherByNameCallback(context.Background(), func(sender *moduleimpl.WatcherSenderList) {
		if fn == nil || sender == nil {
			return
		}
		fn(&DiscoveryWatcherSender{
			Module:     m,
			EtcdHeader: sender.EtcdHeader,
			Node:       toNodeInfo(sender),
		})
	})
}

func (m *etcdModuleAdapter) AddTopologyWatcher(fn TopologyWatcherListCallback) error {
	if m.impl == nil {
		return nil
	}
	return m.impl.AddTopologyWatcher(context.Background(), func(sender *moduleimpl.TopologyWatcherSender) {
		if fn == nil || sender == nil {
			return
		}
		topology := toTopologyInfo(moduleimpl.BuildTopologyCompatEvent(sender))
		if topology == nil {
			return
		}
		fn(&TopologyWatcherSender{
			Module:     m,
			EtcdHeader: sender.EtcdHeader,
			Topology:   topology,
		})
	})
}

func (m *etcdModuleAdapter) GetLastEtcdEventDiscoveryHeader() *EtcdResponseHeader {
	if m.impl == nil {
		return nil
	}
	return m.impl.GetLastEtcdEventHeader()
}

func (m *etcdModuleAdapter) GetLastEtcdEventTopologyHeader() *EtcdResponseHeader {
	if m.impl == nil {
		return nil
	}
	return m.impl.GetLastEtcdEventTopologyHeader()
}

func (m *etcdModuleAdapter) AddRegistrationActor(val *string, nodePath string) *EtcdRegistration {
	if m.impl == nil || val == nil {
		return nil
	}
	actor, err := m.impl.AddRegistrationByValueDefaultTTL(context.Background(), *val, nodePath)
	if err != nil {
		return nil
	}
	return actor
}

func (m *etcdModuleAdapter) RemoveRegistrationActor(registration *EtcdRegistration) bool {
	if m.impl == nil || registration == nil {
		return false
	}
	path := registration.GetPath()
	if path == "" {
		return false
	}
	if err := m.impl.RemoveRegistration(context.Background(), path); err != nil {
		return false
	}
	return true
}

func (m *etcdModuleAdapter) AddOnNodeDiscoveryEvent(fn NodeEventCallback) EventCallbackHandle {
	if m.impl == nil || fn == nil {
		return 0
	}
	h := m.impl.AddOnNodeDiscoveryEventCompat(func(action moduleimpl.DiscoveryAction, node *discovery.DiscoveryNode) {
		fn(action, node)
	})
	return EventCallbackHandle(h)
}

func (m *etcdModuleAdapter) RemoveOnNodeEvent(handle EventCallbackHandle) {
	if m.impl == nil {
		return
	}
	m.impl.RemoveOnNodeEvent(events.EventCallbackHandle(handle))
}

func (m *etcdModuleAdapter) AddOnTopologyInfoEvent(fn TopologyInfoEventCallback) EventCallbackHandle {
	if m.impl == nil || fn == nil {
		return 0
	}
	h := m.impl.AddOnTopologyInfoEvent(func(sender *moduleimpl.TopologyWatcherSender) {
		if sender == nil {
			return
		}
		compat := moduleimpl.BuildTopologyCompatEvent(sender)
		if compat == nil || compat.Storage == nil || compat.Storage.Info == nil {
			return
		}
		version := toEtcdDataVersion(compat.Storage)
		fn(compat.Action, compat.Storage.Info, &version)
	})
	return EventCallbackHandle(h)
}

func (m *etcdModuleAdapter) RemoveOnTopologyInfoEvent(handle EventCallbackHandle) {
	if m.impl == nil {
		return
	}
	m.impl.RemoveOnTopologyInfoEvent(events.EventCallbackHandle(handle))
}

func (m *etcdModuleAdapter) GetGlobalDiscovery() *EtcdDiscoverySet {
	if m.impl == nil {
		return nil
	}
	discoverySet, err := m.impl.GetGlobalDiscovery()
	if err != nil {
		return nil
	}
	return discoverySet
}

func (m *etcdModuleAdapter) GetTopologyInfoSet() map[uint64]*TopologyStorage {
	ret := map[uint64]*TopologyStorage{}
	if m.impl == nil {
		return ret
	}
	for id, item := range m.impl.GetTopologyInfoSetCompat() {
		if item == nil || item.Info == nil {
			continue
		}
		ret[id] = &TopologyStorage{
			Info:    item.Info,
			Version: toEtcdDataVersion(item),
		}
	}
	return ret
}

func (m *etcdModuleAdapter) HasDiscoverySnapshot() bool {
	if m.impl == nil {
		return false
	}
	return m.impl.HasSnapshot()
}

func (m *etcdModuleAdapter) AddOnLoadDiscoverySnapshot(fn DiscoverySnapshotEventCallback) EventCallbackHandle {
	if m.impl == nil || fn == nil {
		return 0
	}
	h := m.impl.AddOnLoadSnapshotCompat(func(_ *moduleimpl.EtcdModule) {
		fn(m)
	})
	return EventCallbackHandle(h)
}

func (m *etcdModuleAdapter) RemoveOnLoadDiscoverySnapshot(handle EventCallbackHandle) {
	if m.impl == nil {
		return
	}
	m.impl.RemoveOnLoadSnapshot(events.EventCallbackHandle(handle))
}

func (m *etcdModuleAdapter) AddOnDiscoverySnapshotLoaded(fn DiscoverySnapshotEventCallback) EventCallbackHandle {
	if m.impl == nil || fn == nil {
		return 0
	}
	h := m.impl.AddOnSnapshotLoadedCompat(func(_ *moduleimpl.EtcdModule) {
		fn(m)
	})
	return EventCallbackHandle(h)
}

func (m *etcdModuleAdapter) RemoveOnDiscoverySnapshotLoaded(handle EventCallbackHandle) {
	if m.impl == nil {
		return
	}
	m.impl.RemoveOnSnapshotLoaded(events.EventCallbackHandle(handle))
}

func (m *etcdModuleAdapter) HasTopologySnapshot() bool {
	if m.impl == nil {
		return false
	}
	return m.impl.HasTopologySnapshot()
}

func (m *etcdModuleAdapter) AddOnLoadTopologySnapshot(fn TopologySnapshotEventCallback) EventCallbackHandle {
	if m.impl == nil || fn == nil {
		return 0
	}
	h := m.impl.AddOnLoadTopologySnapshot(func(_ *moduleimpl.EtcdModule) {
		fn(m)
	})
	return EventCallbackHandle(h)
}

func (m *etcdModuleAdapter) RemoveOnLoadTopologySnapshot(handle EventCallbackHandle) {
	if m.impl == nil {
		return
	}
	m.impl.RemoveOnLoadTopologySnapshot(events.EventCallbackHandle(handle))
}

func (m *etcdModuleAdapter) AddOnTopologySnapshotLoaded(fn TopologySnapshotEventCallback) EventCallbackHandle {
	if m.impl == nil || fn == nil {
		return 0
	}
	h := m.impl.AddOnTopologySnapshotLoaded(func(_ *moduleimpl.EtcdModule) {
		fn(m)
	})
	return EventCallbackHandle(h)
}

func (m *etcdModuleAdapter) RemoveOnTopologySnapshotLoaded(handle EventCallbackHandle) {
	if m.impl == nil {
		return
	}
	m.impl.RemoveOnTopologySnapshotLoaded(events.EventCallbackHandle(handle))
}

func toNodeInfo(sender *moduleimpl.WatcherSenderList) *NodeInfo {
	if sender == nil || sender.Node == nil {
		return nil
	}
	return &NodeInfo{
		NodeDiscovery: sender.Node.Info,
		Action:        sender.Action,
	}
}

func toEtcdDataVersion(storage *moduleimpl.TopologyCompatStorage) EtcdDataVersion {
	if storage == nil {
		return EtcdDataVersion{}
	}
	return EtcdDataVersion{
		CreateRevision: storage.CreateRevision,
		ModRevision:    storage.ModRevision,
		Version:        storage.Version,
	}
}

func toTopologyInfo(compat *moduleimpl.TopologyCompatEvent) *TopologyInfo {
	if compat == nil || compat.Storage == nil || compat.Storage.Info == nil {
		return nil
	}
	return &TopologyInfo{
		Storage: TopologyStorage{
			Info:    compat.Storage.Info,
			Version: toEtcdDataVersion(compat.Storage),
		},
		Action: compat.Action,
	}
}
