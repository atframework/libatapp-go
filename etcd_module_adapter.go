package libatapp

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

var _ EtcdAppModuleImpl = (*etcdModuleAdapter)(nil)

// etcdModuleAdapter bridges the legacy EtcdModuleImpl interface to the new v2
// Actor/CSP EtcdModule.  Config state that the new v2 does not expose is held
// locally in this struct.
type etcdModuleAdapter struct {
	AppModuleBase

	mu   sync.RWMutex
	impl *modulev2.EtcdModule

	// config state (new v2 does not expose these)
	etcdCfg    *pb.AtappEtcd
	pathCfg    modulev2.PathConfig
	customData string
	enabled    bool

	// callback registry
	cbMu  sync.Mutex
	cbSeq atomic.Int64

	nodeEventCbs map[EventCallbackHandle]NodeEventCallback
	topoEventCbs map[EventCallbackHandle]TopologyInfoEventCallback

	discSnapLoadCbs   map[EventCallbackHandle]DiscoverySnapshotEventCallback
	discSnapLoadedCbs map[EventCallbackHandle]DiscoverySnapshotEventCallback

	topoSnapLoadCbs   map[EventCallbackHandle]TopologySnapshotEventCallback
	topoSnapLoadedCbs map[EventCallbackHandle]TopologySnapshotEventCallback

	// per-event watcher callbacks (registered before Start; subscribed on Init)
	byIDWatcherCbs   []DiscoveryWatcherListCallback
	byNameWatcherCbs []DiscoveryWatcherListCallback
	topoWatcherCbs   []TopologyWatcherListCallback

	// prevSnap is the last snapshot seen by onSnapshotPublished; used for diff.
	// Written only on ProjectionActor's goroutine (via OnSnapshotPublished hook).
	prevSnap atomic.Pointer[modulev2.ExportSnapshot]

	// registrations caches every ServiceInfo submitted via AddRegistration* so
	// that they can be replayed after a hard reload (new impl, new actor state).
	// Protected by mu.
	registrations map[string]modulev2.ServiceInfo
}

func newEtcdModuleAdapter(owner AppImpl) *etcdModuleAdapter {
	a := &etcdModuleAdapter{
		AppModuleBase:     CreateAppModuleBase(owner),
		nodeEventCbs:      make(map[EventCallbackHandle]NodeEventCallback),
		topoEventCbs:      make(map[EventCallbackHandle]TopologyInfoEventCallback),
		discSnapLoadCbs:   make(map[EventCallbackHandle]DiscoverySnapshotEventCallback),
		discSnapLoadedCbs: make(map[EventCallbackHandle]DiscoverySnapshotEventCallback),
		topoSnapLoadCbs:   make(map[EventCallbackHandle]TopologySnapshotEventCallback),
		topoSnapLoadedCbs: make(map[EventCallbackHandle]TopologySnapshotEventCallback),
		registrations:     make(map[string]modulev2.ServiceInfo),
	}
	return a
}

// nextCbHandle allocates a unique EventCallbackHandle.
func (m *etcdModuleAdapter) nextCbHandle() EventCallbackHandle {
	return EventCallbackHandle(m.cbSeq.Add(1))
}

// ensureImpl lazily creates the v2 EtcdModule from the app's config.
// Caller must NOT hold m.mu.
func (m *etcdModuleAdapter) ensureImpl() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.impl != nil {
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
	etcdCfg := cfg.ConfigPb.GetEtcd()
	if etcdCfg == nil {
		return nil
	}

	// Always persist the config for GetConfigure() / GetConfigurePath() queries,
	// even when the module is disabled.
	m.etcdCfg = etcdCfg
	m.enabled = etcdCfg.GetEnable()

	if !m.enabled {
		// Module is administratively disabled; do not create the impl.
		return nil
	}

	opts := modulev2.ModuleOptions{
		OnSnapshotPublished: m.onSnapshotPublished,
	}
	if cl := etcdCfg.GetCluster(); cl != nil {
		if ri := cl.GetRetryInterval(); ri != nil {
			opts.RetryInterval = ri.AsDuration()
		}
	}
	if opts.RetryInterval <= 0 {
		if kp := etcdCfg.GetKeepalive(); kp != nil {
			if ri := kp.GetRetryInterval(); ri != nil {
				opts.RetryInterval = ri.AsDuration()
			}
		}
	}

	etcdModule, err := modulev2.NewEtcdModuleFromConfig(etcdCfg, nil, opts)
	if err != nil {
		return err
	}
	m.impl = etcdModule
	// Derive pathCfg from the module itself to guarantee single source of truth.
	m.pathCfg = etcdModule.GetPathConfig()
	return nil
}

// ── AppModuleImpl ─────────────────────────────────────────────────────────

func (m *etcdModuleAdapter) Name() string { return "etcd_module" }

func (m *etcdModuleAdapter) Init(parent context.Context) error {
	if err := m.ensureImpl(); err != nil {
		return err
	}
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return nil
	}
	return m.startImpl(parent, impl)
}

// startImpl starts impl and replays all pending registrations from the cache.
// Must be called with m.mu NOT held.
func (m *etcdModuleAdapter) startImpl(ctx context.Context, impl *modulev2.EtcdModule) error {
	if err := impl.Start(ctx); err != nil {
		return err
	}

	return m.replayRegistrations(ctx, impl)
}

// replayRegistrations re-submits every cached ServiceInfo to impl and waits
// for all writes to complete (or ctx to expire).
// Call after impl.Start to restore registrations lost when the actor restarts.
// Must be called with m.mu NOT held.
func (m *etcdModuleAdapter) replayRegistrations(ctx context.Context, impl *modulev2.EtcdModule) error {
	m.mu.RLock()
	if len(m.registrations) == 0 {
		m.mu.RUnlock()
		return nil
	}
	svcs := make([]modulev2.ServiceInfo, 0, len(m.registrations))
	for _, svc := range m.registrations {
		svcs = append(svcs, svc)
	}
	m.mu.RUnlock()

	handles := make([]*modulev2.RegistrationHandle, 0, len(svcs))
	for _, svc := range svcs {
		h, err := impl.RegisterService(ctx, svc)
		if err != nil {
			continue
		}
		handles = append(handles, h)
	}
	for _, h := range handles {
		if err := h.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *etcdModuleAdapter) Reload() error {
	// Classify the change: does it require Stop+Start (hard) or just endpoint
	// update (soft)?  Soft is preferred because it preserves the lease and
	// keeps watch streams live.
	owner := m.GetApp()
	var newEtcdCfg *pb.AtappEtcd
	if owner != nil {
		if cfg := owner.GetConfig(); cfg != nil && cfg.ConfigPb != nil {
			newEtcdCfg = cfg.ConfigPb.GetEtcd()
		}
	}

	m.mu.RLock()
	oldEtcdCfg := m.etcdCfg
	impl := m.impl
	m.mu.RUnlock()

	if etcdHardReloadRequired(oldEtcdCfg, newEtcdCfg) {
		return m.hardReload()
	}

	// ── Soft path ────────────────────────────────────────────────────────
	// TLS, auth, base path and the enable flag are unchanged.
	// Only hosts list (and/or timing params) may differ.
	m.mu.Lock()
	m.etcdCfg = newEtcdCfg
	m.mu.Unlock()

	if impl == nil {
		return nil
	}
	if newHosts := newEtcdCfg.GetHosts(); len(newHosts) > 0 {
		err := impl.UpdateEndpoints(newHosts)
		if err != nil {
			return err
		}
	}
	return nil
}

// hardReload tears down the running impl and all cached state, then re-creates
// it from the current app config.  Use when TLS, auth, base path, or the
// enable flag changes — scenarios that cannot be hot-swapped on a live client.
func (m *etcdModuleAdapter) hardReload() error {
	m.mu.Lock()
	impl := m.impl
	oldPathCfg := m.pathCfg
	m.impl = nil
	m.etcdCfg = nil
	m.pathCfg = modulev2.PathConfig{}
	m.mu.Unlock()

	m.prevSnap.Store(nil)

	if impl != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		err := impl.Stop(stopCtx)
		if err != nil {
			return err
		}
	}

	// Re-create from current config (no-op when there is no owner or the
	// module is disabled in the new config).
	if err := m.ensureImpl(); err != nil {
		return err
	}

	// If the path prefixes changed the cached ServiceInfo.Path values are
	// stale (they embed the old prefix).  Clear them so that replayRegistrations
	// does not write ghost keys under the new lease; the application layer will
	// re-register with paths built from the new prefix.
	m.mu.Lock()
	newPathCfg := m.pathCfg
	newImpl := m.impl
	if oldPathCfg.ByIDPrefix != newPathCfg.ByIDPrefix ||
		oldPathCfg.ByNamePrefix != newPathCfg.ByNamePrefix ||
		oldPathCfg.TopologyPrefix != newPathCfg.TopologyPrefix {
		m.registrations = make(map[string]modulev2.ServiceInfo)
	}
	m.mu.Unlock()

	if newImpl == nil {
		return nil
	}
	return m.startImpl(context.Background(), newImpl)
}

func (m *etcdModuleAdapter) Stop() (bool, error) {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return true, nil
	}
	return true, impl.Stop(context.Background())
}

func (m *etcdModuleAdapter) Cleanup() {
	m.mu.Lock()
	impl := m.impl
	m.impl = nil
	m.mu.Unlock()

	if impl != nil {
		_ = impl.Stop(context.Background())
	}
}

func (m *etcdModuleAdapter) Tick(_ context.Context) bool {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return false
	}
	impl.Tick()
	return false
}

func (m *etcdModuleAdapter) Reset() {
	m.mu.Lock()
	impl := m.impl
	m.impl = nil
	m.etcdCfg = nil
	m.pathCfg = modulev2.PathConfig{}
	m.mu.Unlock()

	m.prevSnap.Store(nil)

	if impl != nil {
		_ = impl.Stop(context.Background())
	}
}

// ── EtcdModuleImpl — config ───────────────────────────────────────────────

func (m *etcdModuleAdapter) GetConfCustomData() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.customData
}

func (m *etcdModuleAdapter) SetConfCustomData(v string) {
	m.mu.Lock()
	m.customData = v
	m.mu.Unlock()
}

func (m *etcdModuleAdapter) GetConfigure() *pb.AtappEtcd {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.etcdCfg
}

func (m *etcdModuleAdapter) GetConfigurePath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.etcdCfg == nil {
		return ""
	}
	return m.etcdCfg.GetPath()
}

// ── EtcdModuleImpl — enable/disable ──────────────────────────────────────

func (m *etcdModuleAdapter) IsEtcdEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enabled
}

func (m *etcdModuleAdapter) EnableEtcd() {
	m.mu.Lock()
	m.enabled = true
	m.mu.Unlock()
}

func (m *etcdModuleAdapter) DisableEtcd() {
	m.mu.Lock()
	m.enabled = false
	m.mu.Unlock()
}

// ── EtcdModuleImpl — keepalive trigger ───────────────────────────────────

func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveTopologyValue() {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl != nil {
		impl.SyncTopology()
	}
}

func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveDiscoveryValue()    {}
func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveDiscoveryArea()     {}
func (m *etcdModuleAdapter) SetMaybeUpdateKeepaliveDiscoveryMetadata() {}

// ── EtcdModuleImpl — path accessors ──────────────────────────────────────

func (m *etcdModuleAdapter) GetDiscoveryByIdPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pathCfg.ByIDPrefix
}

func (m *etcdModuleAdapter) GetDiscoveryByNamePath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pathCfg.ByNamePrefix
}

func (m *etcdModuleAdapter) GetTopologyPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pathCfg.TopologyPrefix
}

func (m *etcdModuleAdapter) GetDiscoveryByIdWatcherPath() string {
	return m.GetDiscoveryByIdPath()
}

func (m *etcdModuleAdapter) GetDiscoveryByNameWatcherPath() string {
	return m.GetDiscoveryByNamePath()
}

func (m *etcdModuleAdapter) GetTopologyWatcherPath() string {
	return m.GetTopologyPath()
}

// ── EtcdModuleImpl — watcher callbacks ───────────────────────────────────

func (m *etcdModuleAdapter) AddDiscoveryWatcherById(fn DiscoveryWatcherListCallback) error {
	if fn == nil {
		return nil
	}
	m.cbMu.Lock()
	m.byIDWatcherCbs = append(m.byIDWatcherCbs, fn)
	m.cbMu.Unlock()
	return nil
}

func (m *etcdModuleAdapter) AddDiscoveryWatcherByName(fn DiscoveryWatcherListCallback) error {
	if fn == nil {
		return nil
	}
	m.cbMu.Lock()
	m.byNameWatcherCbs = append(m.byNameWatcherCbs, fn)
	m.cbMu.Unlock()
	return nil
}

func (m *etcdModuleAdapter) AddTopologyWatcher(fn TopologyWatcherListCallback) error {
	if fn == nil {
		return nil
	}
	m.cbMu.Lock()
	m.topoWatcherCbs = append(m.topoWatcherCbs, fn)
	m.cbMu.Unlock()
	return nil
}

// ── EtcdModuleImpl — registration ─────────────────────────────────────────

func (m *etcdModuleAdapter) AddRegistrationDiscoveryActor(val *pb.AtappDiscovery, nodePath string) *EtcdRegistration {
	if val == nil {
		return nil
	}
	svc := modulev2.ServiceInfo{Discovery: val, Path: nodePath}

	m.mu.Lock()
	m.registrations[nodePath] = svc
	impl := m.impl
	m.mu.Unlock()

	if impl == nil {
		return nil
	}
	h, err := impl.RegisterService(context.Background(), svc)
	if err != nil {
		return nil
	}
	if err = h.Wait(context.Background()); err != nil {
		return nil
	}
	return &EtcdRegistration{path: nodePath}
}

func (m *etcdModuleAdapter) AddRegistrationTopologyActor(val *pb.AtappTopologyInfo, nodePath string) *EtcdRegistration {
	if val == nil {
		return nil
	}
	svc := modulev2.ServiceInfo{TopologyInfo: val, Path: nodePath}

	m.mu.Lock()
	m.registrations[nodePath] = svc
	impl := m.impl
	m.mu.Unlock()

	if impl == nil {
		return nil
	}
	h, err := impl.RegisterService(context.Background(), svc)
	if err != nil {
		return nil
	}
	if err = h.Wait(context.Background()); err != nil {
		return nil
	}
	return &EtcdRegistration{path: nodePath}
}

func (m *etcdModuleAdapter) RemoveRegistrationActor(reg *EtcdRegistration) bool {
	if reg == nil {
		return false
	}
	path := reg.GetPath()
	if path == "" {
		return false
	}

	m.mu.Lock()
	delete(m.registrations, path)
	impl := m.impl
	m.mu.Unlock()

	if impl == nil {
		return false
	}
	if err := impl.UnregisterService(context.Background(), path); err != nil {
		return false
	}
	return true
}

// ── EtcdModuleImpl — node discovery event callbacks ───────────────────────

func (m *etcdModuleAdapter) AddOnNodeDiscoveryEvent(fn NodeEventCallback) EventCallbackHandle {
	if fn == nil {
		return 0
	}
	h := m.nextCbHandle()
	m.cbMu.Lock()
	m.nodeEventCbs[h] = fn
	m.cbMu.Unlock()
	return h
}

func (m *etcdModuleAdapter) RemoveOnNodeEvent(handle EventCallbackHandle) {
	m.cbMu.Lock()
	delete(m.nodeEventCbs, handle)
	m.cbMu.Unlock()
}

// ── EtcdModuleImpl — topology info event callbacks ────────────────────────

func (m *etcdModuleAdapter) AddOnTopologyInfoEvent(fn TopologyInfoEventCallback) EventCallbackHandle {
	if fn == nil {
		return 0
	}
	h := m.nextCbHandle()
	m.cbMu.Lock()
	m.topoEventCbs[h] = fn
	m.cbMu.Unlock()
	return h
}

func (m *etcdModuleAdapter) RemoveOnTopologyInfoEvent(handle EventCallbackHandle) {
	m.cbMu.Lock()
	delete(m.topoEventCbs, handle)
	m.cbMu.Unlock()
}

// ── EtcdModuleImpl — global discovery ────────────────────────────────────

// GetGlobalDiscovery returns nil; use GetSnapshot().Discovery for a read-model.
func (m *etcdModuleAdapter) GetGlobalDiscovery() *EtcdDiscoverySet { return nil }

// ── EtcdModuleImpl — topology info set ───────────────────────────────────

// GetTopologyInfoSet returns a shallow copy of the topology cache from the latest snapshot.
func (m *etcdModuleAdapter) GetTopologyInfoSet() map[uint64]*TopologyStorage {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return make(map[uint64]*TopologyStorage)
	}
	snap := impl.GetSnapshot()
	if snap == nil {
		return make(map[uint64]*TopologyStorage)
	}
	out := make(map[uint64]*TopologyStorage, len(snap.Topology.NodesByID))
	for id, node := range snap.Topology.NodesByID {
		if node == nil || node.Info == nil {
			continue
		}
		out[id] = &TopologyStorage{
			Info: node.Info,
			Version: EtcdDataVersion{
				CreateRevision: node.CreateRevision,
				ModRevision:    node.ModRevision,
				Version:        node.Version,
			},
		}
	}
	return out
}

// GetDiscoveryNodeSet returns a shallow copy of the discovery node cache from the latest snapshot,
// keyed by etcd path.
func (m *etcdModuleAdapter) GetDiscoveryNodeSet() map[string]*DiscoveryNodeStorage {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return make(map[string]*DiscoveryNodeStorage)
	}
	snap := impl.GetSnapshot()
	if snap == nil {
		return make(map[string]*DiscoveryNodeStorage)
	}
	out := make(map[string]*DiscoveryNodeStorage, len(snap.Discovery.NodesByPath))
	for path, node := range snap.Discovery.NodesByPath {
		if node == nil || node.Info == nil {
			continue
		}
		out[path] = &DiscoveryNodeStorage{
			Info: node.Info,
			Version: EtcdDataVersion{
				CreateRevision: node.CreateRevision,
				ModRevision:    node.ModRevision,
				Version:        node.Version,
			},
		}
	}
	return out
}

// ── EtcdModuleImpl — discovery snapshot ──────────────────────────────────

func (m *etcdModuleAdapter) HasDiscoverySnapshot() bool {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return false
	}
	snap := impl.GetSnapshot()
	return snap != nil && snap.Discovery.Ready
}

func (m *etcdModuleAdapter) AddOnLoadDiscoverySnapshot(fn DiscoverySnapshotEventCallback) EventCallbackHandle {
	if fn == nil {
		return 0
	}
	h := m.nextCbHandle()
	m.cbMu.Lock()
	m.discSnapLoadCbs[h] = fn
	m.cbMu.Unlock()
	return h
}

func (m *etcdModuleAdapter) RemoveOnLoadDiscoverySnapshot(handle EventCallbackHandle) {
	m.cbMu.Lock()
	delete(m.discSnapLoadCbs, handle)
	m.cbMu.Unlock()
}

func (m *etcdModuleAdapter) AddOnDiscoverySnapshotLoaded(fn DiscoverySnapshotEventCallback) EventCallbackHandle {
	if fn == nil {
		return 0
	}
	h := m.nextCbHandle()
	m.cbMu.Lock()
	m.discSnapLoadedCbs[h] = fn
	m.cbMu.Unlock()
	return h
}

func (m *etcdModuleAdapter) RemoveOnDiscoverySnapshotLoaded(handle EventCallbackHandle) {
	m.cbMu.Lock()
	delete(m.discSnapLoadedCbs, handle)
	m.cbMu.Unlock()
}

// ── EtcdModuleImpl — topology snapshot ───────────────────────────────────

func (m *etcdModuleAdapter) HasTopologySnapshot() bool {
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return false
	}
	snap := impl.GetSnapshot()
	return snap != nil && snap.Topology.Ready
}

func (m *etcdModuleAdapter) AddOnLoadTopologySnapshot(fn TopologySnapshotEventCallback) EventCallbackHandle {
	if fn == nil {
		return 0
	}
	h := m.nextCbHandle()
	m.cbMu.Lock()
	m.topoSnapLoadCbs[h] = fn
	m.cbMu.Unlock()
	return h
}

func (m *etcdModuleAdapter) RemoveOnLoadTopologySnapshot(handle EventCallbackHandle) {
	m.cbMu.Lock()
	delete(m.topoSnapLoadCbs, handle)
	m.cbMu.Unlock()
}

func (m *etcdModuleAdapter) AddOnTopologySnapshotLoaded(fn TopologySnapshotEventCallback) EventCallbackHandle {
	if fn == nil {
		return 0
	}
	h := m.nextCbHandle()
	m.cbMu.Lock()
	m.topoSnapLoadedCbs[h] = fn
	m.cbMu.Unlock()
	return h
}

func (m *etcdModuleAdapter) RemoveOnTopologySnapshotLoaded(handle EventCallbackHandle) {
	m.cbMu.Lock()
	delete(m.topoSnapLoadedCbs, handle)
	m.cbMu.Unlock()
}

// ── Snapshot diff handler ─────────────────────────────────────────────────

// onSnapshotPublished is called by ProjectionActor after each atomic snapshot
// store.  Runs on ProjectionActor's goroutine; must be fast and non-blocking.
func (m *etcdModuleAdapter) onSnapshotPublished(snap *modulev2.ExportSnapshot) {
	prev := m.prevSnap.Swap(snap)
	if snap == nil {
		return
	}

	switch snap.Cause {
	case modulev2.SnapshotCauseDiscovery:
		m.diffDiscovery(prev, snap)
	case modulev2.SnapshotCauseTopology:
		m.diffTopology(prev, snap)
	default: // SnapshotCauseReset or unknown
		m.diffDiscovery(prev, snap)
		m.diffTopology(prev, snap)
	}
}

// diffDiscovery diffs the Discovery sub-tree and fires appropriate callbacks.
func (m *etcdModuleAdapter) diffDiscovery(prev, snap *modulev2.ExportSnapshot) {
	prevDiscReady := prev != nil && prev.Discovery.Ready
	newDiscReady := snap.Discovery.Ready

	// ready → not ready
	if prevDiscReady && !newDiscReady {
		m.fireDiscSnapLoadCbs()
	}

	// NodesByPath diff
	for path, newNode := range snap.Discovery.NodesByPath {
		if newNode == nil || newNode.Info == nil {
			continue
		}
		if prev != nil {
			if oldNode, exists := prev.Discovery.NodesByPath[path]; exists && oldNode != nil {
				if atappDiscoveryEqual(oldNode.Info, newNode.Info) {
					continue // content unchanged – suppress redundant callback
				}
			}
		}
		m.fireNodeCallbacks(EtcdDiscoveryActionPut, newNode)
	}
	if prev != nil {
		for path, oldNode := range prev.Discovery.NodesByPath {
			if oldNode == nil {
				continue
			}
			if _, exists := snap.Discovery.NodesByPath[path]; !exists {
				m.fireNodeCallbacks(EtcdDiscoveryActionDelete, oldNode)
			}
		}
	}

	// not ready → ready
	if !prevDiscReady && newDiscReady {
		m.fireDiscSnapLoadedCbs()
	}
}

// diffTopology diffs the Topology sub-tree and fires appropriate callbacks.
func (m *etcdModuleAdapter) diffTopology(prev, snap *modulev2.ExportSnapshot) {
	prevTopoReady := prev != nil && prev.Topology.Ready
	newTopoReady := snap.Topology.Ready

	// ready → not ready
	if prevTopoReady && !newTopoReady {
		m.fireTopologySnapLoadCbs()
	}

	// NodesByID diff
	for id, newNode := range snap.Topology.NodesByID {
		if newNode == nil || newNode.Info == nil || id == 0 {
			continue
		}
		if prev != nil {
			if oldNode, exists := prev.Topology.NodesByID[id]; exists && oldNode != nil {
				if atappTopologyEqual(oldNode.Info, newNode.Info) {
					continue // content unchanged – suppress redundant callback
				}
			}
		}
		m.fireTopologyCallbacks(EtcdWatchEventPut, newNode)
	}
	if prev != nil {
		for id, oldNode := range prev.Topology.NodesByID {
			if oldNode == nil || id == 0 {
				continue
			}
			if _, exists := snap.Topology.NodesByID[id]; !exists {
				m.fireTopologyCallbacks(EtcdWatchEventDelete, oldNode)
			}
		}
	}

	// not ready → ready
	if !prevTopoReady && newTopoReady {
		m.fireTopologySnapLoadedCbs()
	}
}

// fireNodeCallbacks fires discovery-node callbacks for a single node change.
// byIDWatcherCbs is only fired for nodes whose path starts with ByIDPrefix;
// byNameWatcherCbs is only fired for nodes whose path starts with ByNamePrefix.
// When the path prefixes are not configured (empty strings), both groups are
// fired to preserve backward-compatible behaviour.
func (m *etcdModuleAdapter) fireNodeCallbacks(action EtcdDiscoveryAction, node *modulev2.DiscoveryNode) {
	nodeInfo := &NodeInfo{NodeDiscovery: node.Info, Action: action}

	// Determine which watcher group owns this node.
	m.mu.RLock()
	byIDPrefix := m.pathCfg.ByIDPrefix
	byNamePrefix := m.pathCfg.ByNamePrefix
	m.mu.RUnlock()

	isById := byIDPrefix != "" && strings.HasPrefix(node.Path, byIDPrefix)
	isByName := byNamePrefix != "" && strings.HasPrefix(node.Path, byNamePrefix)
	// When prefixes are unconfigured, fall back to firing both groups.
	fireAll := !isById && !isByName

	m.cbMu.Lock()
	nodeCbs := make([]NodeEventCallback, 0, len(m.nodeEventCbs))
	for _, fn := range m.nodeEventCbs {
		nodeCbs = append(nodeCbs, fn)
	}
	var byIDCbs []DiscoveryWatcherListCallback
	var byNameCbs []DiscoveryWatcherListCallback
	if isById || fireAll {
		byIDCbs = make([]DiscoveryWatcherListCallback, len(m.byIDWatcherCbs))
		copy(byIDCbs, m.byIDWatcherCbs)
	}
	if isByName || fireAll {
		byNameCbs = make([]DiscoveryWatcherListCallback, len(m.byNameWatcherCbs))
		copy(byNameCbs, m.byNameWatcherCbs)
	}
	m.cbMu.Unlock()

	sender := &DiscoveryWatcherSender{Module: m, Node: nodeInfo}
	for _, fn := range nodeCbs {
		fn(action, node)
	}
	for _, fn := range byIDCbs {
		fn(sender)
	}
	for _, fn := range byNameCbs {
		fn(sender)
	}
}

// fireTopologyCallbacks fires all topology callbacks for a single node change.
func (m *etcdModuleAdapter) fireTopologyCallbacks(eventType EtcdWatchEvent, node *modulev2.TopologyNode) {
	var topologyStorage *TopologyStorage
	if node.Info != nil {
		topologyStorage = &TopologyStorage{
			Info: node.Info,
			Version: EtcdDataVersion{
				CreateRevision: node.CreateRevision,
				ModRevision:    node.ModRevision,
				Version:        node.Version,
			},
		}
	}
	version := &EtcdDataVersion{
		CreateRevision: node.CreateRevision,
		ModRevision:    node.ModRevision,
		Version:        node.Version,
	}

	m.cbMu.Lock()
	topologyInfoCallbacks := make([]TopologyInfoEventCallback, 0, len(m.topoEventCbs))
	for _, fn := range m.topoEventCbs {
		topologyInfoCallbacks = append(topologyInfoCallbacks, fn)
	}
	topologyWatcherCallbacks := make([]TopologyWatcherListCallback, len(m.topoWatcherCbs))
	copy(topologyWatcherCallbacks, m.topoWatcherCbs)
	m.cbMu.Unlock()

	sender := &TopologyWatcherSender{Module: m, Topology: topologyStorage, Action: eventType}
	for _, fn := range topologyInfoCallbacks {
		fn(eventType, node.Info, version)
	}
	for _, fn := range topologyWatcherCallbacks {
		fn(sender)
	}
}

func (m *etcdModuleAdapter) fireDiscSnapLoadCbs() {
	m.cbMu.Lock()
	cbs := make([]DiscoverySnapshotEventCallback, 0, len(m.discSnapLoadCbs))
	for _, fn := range m.discSnapLoadCbs {
		cbs = append(cbs, fn)
	}
	m.cbMu.Unlock()
	for _, fn := range cbs {
		fn(m)
	}
}

func (m *etcdModuleAdapter) fireDiscSnapLoadedCbs() {
	m.cbMu.Lock()
	cbs := make([]DiscoverySnapshotEventCallback, 0, len(m.discSnapLoadedCbs))
	for _, fn := range m.discSnapLoadedCbs {
		cbs = append(cbs, fn)
	}
	m.cbMu.Unlock()
	for _, fn := range cbs {
		fn(m)
	}
}

func (m *etcdModuleAdapter) fireTopologySnapLoadCbs() {
	m.cbMu.Lock()
	cbs := make([]TopologySnapshotEventCallback, 0, len(m.topoSnapLoadCbs))
	for _, fn := range m.topoSnapLoadCbs {
		cbs = append(cbs, fn)
	}
	m.cbMu.Unlock()
	for _, fn := range cbs {
		fn(m)
	}
}

func (m *etcdModuleAdapter) fireTopologySnapLoadedCbs() {
	m.cbMu.Lock()
	cbs := make([]TopologySnapshotEventCallback, 0, len(m.topoSnapLoadedCbs))
	for _, fn := range m.topoSnapLoadedCbs {
		cbs = append(cbs, fn)
	}
	m.cbMu.Unlock()
	for _, fn := range cbs {
		fn(m)
	}
}

// ── Proto equality helpers ────────────────────────────────────────────────

// atappTopologyEqual reports whether two AtappTopologyInfo values are logically
// equal for the purpose of change detection.  It mirrors the C++ implementation
// atapp_topology_equal: compares id, upstream_id, name, and the data.label map.
func atappTopologyEqual(l, r *pb.AtappTopologyInfo) bool {
	if l == r {
		return true
	}
	if l == nil || r == nil {
		return false
	}
	if l.GetId() != r.GetId() {
		return false
	}
	if l.GetUpstreamId() != r.GetUpstreamId() {
		return false
	}
	if l.GetName() != r.GetName() {
		return false
	}
	lLabels := l.GetData().GetLabel()
	rLabels := r.GetData().GetLabel()
	if len(lLabels) != len(rLabels) {
		return false
	}
	for k, lv := range lLabels {
		rv, ok := rLabels[k]
		if !ok || lv != rv {
			return false
		}
	}
	return true
}

// atappDiscoveryEqual reports whether two AtappDiscovery values are logically
// equal for the purpose of change detection.  It compares the fields that define
// service identity and routing configuration, using atappAreaEqual for the area
// sub-field.  The Runtime field is intentionally excluded as it carries dynamic
// heartbeat state that does not represent a meaningful service change.
func atappDiscoveryEqual(l, r *pb.AtappDiscovery) bool {
	if l == r {
		return true
	}
	if l == nil || r == nil {
		return false
	}
	if l.GetId() != r.GetId() {
		return false
	}
	if l.GetName() != r.GetName() {
		return false
	}
	if l.GetTypeId() != r.GetTypeId() {
		return false
	}
	if l.GetTypeName() != r.GetTypeName() {
		return false
	}
	if l.GetHostname() != r.GetHostname() {
		return false
	}
	if l.GetPid() != r.GetPid() {
		return false
	}
	if l.GetVersion() != r.GetVersion() {
		return false
	}
	if l.GetCustomData() != r.GetCustomData() {
		return false
	}
	if l.GetIdentity() != r.GetIdentity() {
		return false
	}
	if l.GetHashCode() != r.GetHashCode() {
		return false
	}
	if l.GetAtbusProtocolVersion() != r.GetAtbusProtocolVersion() {
		return false
	}
	if l.GetAtbusProtocolMinVersion() != r.GetAtbusProtocolMinVersion() {
		return false
	}
	if !atappAreaEqual(l.GetArea(), r.GetArea()) {
		return false
	}
	lListen := l.GetListen()
	rListen := r.GetListen()
	if len(lListen) != len(rListen) {
		return false
	}
	for i := range lListen {
		if lListen[i] != rListen[i] {
			return false
		}
	}
	return true
}

// ── Reload classification helpers ────────────────────────────────────────

// etcdHardReloadRequired returns true when the configuration delta between old
// and newCfg requires a full Stop+Start cycle rather than a soft endpoint
// update via UpdateEndpoints.
//
// Hard changes are those that cannot be hot-swapped on a live clientv3 instance:
//
//   - either config is nil (no baseline to compare)
//   - enable flag toggled (true→false must revoke lease, false→true needs new client)
//   - hosts list emptied (no valid endpoint remains)
//   - base path changed (pathCfg is baked into actor key prefixes at construction)
//   - auth credentials changed (clientv3 has no live credential-swap support)
//   - TLS scheme toggled (plaintext↔TLS requires a new clientv3.New call)
//   - CA certificate file changed (new CA may not trust the old cert on reconnect)
func etcdHardReloadRequired(old, newCfg *pb.AtappEtcd) bool {
	if old == nil || newCfg == nil {
		return true
	}
	if old.GetEnable() != newCfg.GetEnable() {
		return true
	}
	if len(newCfg.GetHosts()) == 0 {
		return true
	}
	if old.GetPath() != newCfg.GetPath() {
		return true
	}
	if old.GetAuthorization() != newCfg.GetAuthorization() {
		return true
	}
	if etcdTLSSchemeChanged(old, newCfg) {
		return true
	}
	return false
}

// etcdTLSSchemeChanged returns true when the TLS configuration changes in a
// way that requires a new clientv3 connection:
//   - SSL presence toggled (plaintext ↔ TLS)
//   - CA certificate path changed (a different CA may reject the peer cert on
//     the next reconnect, so a fresh client with the new CA bundle is safer)
func etcdTLSSchemeChanged(old, newCfg *pb.AtappEtcd) bool {
	oldSSL := old.GetSsl()
	newSSL := newCfg.GetSsl()
	// "TLS is active" when there is an ssl block that configures at least one
	// of: client cert, CA cert, or peer verification.
	oldActive := oldSSL != nil && (oldSSL.GetSslClientCert() != "" || oldSSL.GetSslCaCert() != "" || oldSSL.GetVerifyPeer())
	newActive := newSSL != nil && (newSSL.GetSslClientCert() != "" || newSSL.GetSslCaCert() != "" || newSSL.GetVerifyPeer())
	if oldActive != newActive {
		return true // plaintext ↔ TLS
	}
	if !oldActive {
		return false // both plaintext
	}
	// Both use TLS — only a CA cert change forces a hard reload.
	return oldSSL.GetSslCaCert() != newSSL.GetSslCaCert()
}

// atappAreaEqual reports whether two AtappArea values are logically equal.
// It mirrors the C++ implementation named atapp_discovery_equal (which operates
// on atapp_area, despite the name).  The C++ original ends with "return false"
// which is a bug; this implementation correctly returns true when all fields match.
func atappAreaEqual(l, r *pb.AtappArea) bool {
	if l == r {
		return true
	}
	if l == nil || r == nil {
		return false
	}
	if l.GetZoneId() != r.GetZoneId() {
		return false
	}
	if l.GetRegion() != r.GetRegion() {
		return false
	}
	if l.GetDistrict() != r.GetDistrict() {
		return false
	}
	return true
}
