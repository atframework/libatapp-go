package libatapp

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

var _ EtcdAppModuleImpl = (*etcdModuleAdapter)(nil)

// etcdModuleAdapter bridges the legacy EtcdModuleImpl interface to the new v2
// Actor/CSP EtcdModule.  Config state that the new v2 does not expose is held
// locally in this struct.
type etcdModuleAdapter struct {
	AppModuleBase

	mu         sync.RWMutex
	impl       *modulev2.EtcdModule

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

	// snapshot readiness
	discoverySnapshotReady bool
	topologySnapshotReady  bool
	topologyInfoSet        map[uint64]*TopologyStorage
	discoveryNodeSet       map[string]*DiscoveryNodeStorage

	// EventBus subscription handle (unsubscribed on Reset/Cleanup)
	busSubHandle modulev2.EventHandle
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
		topologyInfoSet:   make(map[uint64]*TopologyStorage),
		discoveryNodeSet:  make(map[string]*DiscoveryNodeStorage),
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

	m.etcdCfg = etcdCfg
	m.enabled = etcdCfg.GetEnable()

	basePath := etcdCfg.GetPath()
	leaseTTL := int64(10)
	if kp := etcdCfg.GetKeepalive(); kp != nil {
		if ttl := kp.GetTtl(); ttl != nil {
			if secs := int64(ttl.AsDuration().Seconds()); secs > 0 {
				leaseTTL = secs
			}
		}
	}
	m.pathCfg = modulev2.PathConfig{
		ByNamePrefix:   basePath + "/by_name",
		ByIDPrefix:     basePath + "/by_id",
		TopologyPrefix: basePath + "/topology",
		WatchPrefixes:  []string{basePath + "/by_id", basePath + "/by_name", basePath + "/topology"},
		LeaseTTL:       leaseTTL,
	}

	opts := modulev2.ModuleOptions{}
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
	return nil
}

// ── AppModuleImpl ─────────────────────────────────────────────────────────

func (m *etcdModuleAdapter) Name() string { return "etcd_module" }

func (m *etcdModuleAdapter) Setup(parent context.Context) error {
	_ = parent
	return m.ensureImpl()
}

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
	if err := impl.Start(parent); err != nil {
		return err
	}
	// Subscribe to the EventBus after Start (bus is initialised inside Start).
	handle := impl.Subscribe(m.handleBusEvent)
	m.mu.Lock()
	m.busSubHandle = handle
	m.mu.Unlock()
	return nil
}

func (m *etcdModuleAdapter) Reload() error {
	// v2 does not have an explicit Reload; config changes require a Stop/Start cycle.
	return nil
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

func (m *etcdModuleAdapter) Timeout() {}

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
	handle := m.busSubHandle
	m.impl = nil
	m.busSubHandle = 0
	m.discoverySnapshotReady = false
	m.topologySnapshotReady = false
	m.topologyInfoSet = make(map[uint64]*TopologyStorage)
	m.discoveryNodeSet = make(map[string]*DiscoveryNodeStorage)
	m.mu.Unlock()

	if impl != nil {
		if handle != 0 {
			impl.Unsubscribe(handle)
		}
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
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return nil
	}

	svc := modulev2.ServiceInfo{Discovery: val, Path: nodePath}
	if _, err := impl.RegisterService(context.Background(), svc); err != nil {
		return nil
	}

	// Return a path token for legacy remove flow.
	return &EtcdRegistration{path: nodePath}
}

func (m *etcdModuleAdapter) AddRegistrationTopologyActor(val *pb.AtappTopologyInfo, nodePath string) *EtcdRegistration {
	if val == nil {
		return nil
	}
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
	if impl == nil {
		return nil
	}

	svc := modulev2.ServiceInfo{TopologyInfo: val, Path: nodePath}
	if _, err := impl.RegisterService(context.Background(), svc); err != nil {
		return nil
	}

	// Return a path token compatible with legacy remove flow.
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
	m.mu.RLock()
	impl := m.impl
	m.mu.RUnlock()
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

// GetTopologyInfoSet returns a shallow copy of the in-memory topology cache.
func (m *etcdModuleAdapter) GetTopologyInfoSet() map[uint64]*TopologyStorage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[uint64]*TopologyStorage, len(m.topologyInfoSet))
	for key, value := range m.topologyInfoSet {
		if value == nil {
			continue
		}
		copied := *value
		out[key] = &copied
	}
	return out
}

// GetDiscoveryNodeSet returns a shallow copy of the in-memory discovery node cache,
// keyed by etcd path.
func (m *etcdModuleAdapter) GetDiscoveryNodeSet() map[string]*DiscoveryNodeStorage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]*DiscoveryNodeStorage, len(m.discoveryNodeSet))
	for key, value := range m.discoveryNodeSet {
		if value == nil {
			continue
		}
		copied := *value
		out[key] = &copied
	}
	return out
}

// ── EtcdModuleImpl — discovery snapshot ──────────────────────────────────

func (m *etcdModuleAdapter) HasDiscoverySnapshot() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.discoverySnapshotReady
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
	defer m.mu.RUnlock()
	return m.topologySnapshotReady
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

// ── EventBus handler ──────────────────────────────────────────────────────

// handleBusEvent is called on the publishing actor's goroutine; must be fast.
func (m *etcdModuleAdapter) handleBusEvent(env modulev2.EventEnvelope) {
	switch env.Type {
	case modulev2.EventWatchNodeUp, modulev2.EventWatchNodeUpdate:
		m.dispatchNodeEvent(EtcdDiscoveryActionPut, env)
	case modulev2.EventWatchNodeDown:
		m.dispatchNodeEvent(EtcdDiscoveryActionDelete, env)
	case modulev2.EventWatchTopologyUp, modulev2.EventWatchTopologyUpdate:
		m.dispatchTopologyEvent(EtcdWatchEventPut, env)
	case modulev2.EventWatchTopologyDown:
		m.dispatchTopologyEvent(EtcdWatchEventDelete, env)
	case modulev2.EventWatchSnapshotLoading:
		m.mu.Lock()
		m.discoverySnapshotReady = false
		m.discoveryNodeSet = make(map[string]*DiscoveryNodeStorage)
		m.mu.Unlock()
		m.fireDiscSnapLoadCbs()
	case modulev2.EventWatchSnapshotLoaded:
		payload, _ := toWatchSnapshotLoadedPayload(env.Payload)
		discoveryNodeSet := make(map[string]*DiscoveryNodeStorage)
		if payload != nil {
			for _, node := range payload.Nodes {
				if node == nil || node.Info == nil || node.Path == "" {
					continue
				}
				discoveryNodeSet[node.Path] = &DiscoveryNodeStorage{
					Info: node.Info,
					Version: EtcdDataVersion{
						CreateRevision: node.CreateRevision,
						ModRevision:    node.ModRevision,
						Version:        node.Version,
					},
				}
			}
		}
		m.mu.Lock()
		m.discoverySnapshotReady = true
		m.discoveryNodeSet = discoveryNodeSet
		m.mu.Unlock()
		m.fireDiscSnapLoadedCbs()
	case modulev2.EventWatchTopologySnapshotLoading:
		m.mu.Lock()
		m.topologySnapshotReady = false
		m.topologyInfoSet = make(map[uint64]*TopologyStorage)
		m.mu.Unlock()
		m.fireTopologySnapLoadCbs()
	case modulev2.EventWatchTopologySnapshotLoaded:
		payload, _ := toWatchTopologySnapshotLoadedPayload(env.Payload)
		topologyInfoSet := make(map[uint64]*TopologyStorage)
		if payload != nil {
			for id, node := range payload.Nodes {
				if node == nil || node.Info == nil || id == 0 {
					continue
				}
				topologyInfoSet[id] = &TopologyStorage{
					Info: node.Info,
					Version: EtcdDataVersion{
						CreateRevision: node.CreateRevision,
						ModRevision:    node.ModRevision,
						Version:        node.Version,
					},
				}
			}
		}
		m.mu.Lock()
		m.topologySnapshotReady = true
		m.topologyInfoSet = topologyInfoSet
		m.mu.Unlock()
		m.fireTopologySnapLoadedCbs()
	}
}

func (m *etcdModuleAdapter) dispatchNodeEvent(action EtcdDiscoveryAction, env modulev2.EventEnvelope) {
	payload, ok := toWatchNodePayload(env.Payload)
	if !ok || payload == nil || (action == EtcdDiscoveryActionPut && payload.Value == nil) {
		return
	}

	// Maintain discoveryNodeSet cache before invoking callbacks.
	// For PUT events, capture the previous entry so we can skip callbacks
	// when the discovery content has not actually changed (etcd Version incremented
	// but proto fields are identical).
	var oldDiscovery *pb.AtappDiscovery
	if action == EtcdDiscoveryActionPut && payload.Value != nil {
		storage := &DiscoveryNodeStorage{
			Info: payload.Value,
			Version: EtcdDataVersion{
				CreateRevision: payload.CreateRevision,
				ModRevision:    payload.ModRevision,
				Version:        payload.Version,
			},
		}
		m.mu.Lock()
		if prev := m.discoveryNodeSet[payload.Key]; prev != nil {
			oldDiscovery = prev.Info
		}
		m.discoveryNodeSet[payload.Key] = storage
		m.mu.Unlock()

		// Content unchanged — update cache but suppress redundant callbacks.
		if oldDiscovery != nil && atappDiscoveryEqual(oldDiscovery, payload.Value) {
			return
		}
	} else if action == EtcdDiscoveryActionDelete && payload.Key != "" {
		m.mu.Lock()
		delete(m.discoveryNodeSet, payload.Key)
		m.mu.Unlock()
	}

	// Build the node payload expected by legacy callback signature.
	node := &modulev2.DiscoveryNode{}
	node.Info = payload.Value
	node.Path = payload.Key
	node.CreateRevision = payload.CreateRevision
	node.ModRevision = payload.ModRevision
	node.Version = payload.Version

	nodeInfo := &NodeInfo{NodeDiscovery: payload.Value, Action: action}

	// Snapshot callback lists under lock, then invoke outside lock.
	m.cbMu.Lock()
	nodeCbs := make([]NodeEventCallback, 0, len(m.nodeEventCbs))
	for _, fn := range m.nodeEventCbs {
		nodeCbs = append(nodeCbs, fn)
	}
	byIDCbs := make([]DiscoveryWatcherListCallback, len(m.byIDWatcherCbs))
	copy(byIDCbs, m.byIDWatcherCbs)
	byNameCbs := make([]DiscoveryWatcherListCallback, len(m.byNameWatcherCbs))
	copy(byNameCbs, m.byNameWatcherCbs)
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

func (m *etcdModuleAdapter) dispatchTopologyEvent(eventType EtcdWatchEvent, env modulev2.EventEnvelope) {
	payload, ok := toWatchTopologyPayload(env.Payload)
	if !ok || payload == nil {
		return
	}

	if payload.Value != nil {
		storage := &TopologyStorage{
			Info: payload.Value,
			Version: EtcdDataVersion{
				CreateRevision: payload.CreateRevision,
				ModRevision:    payload.ModRevision,
				Version:        payload.Version,
			},
		}
		m.mu.Lock()
		var oldTopology *pb.AtappTopologyInfo
		if prev := m.topologyInfoSet[payload.Value.GetId()]; prev != nil {
			oldTopology = prev.Info
		}
		m.topologyInfoSet[payload.Value.GetId()] = storage
		m.mu.Unlock()

		// Content unchanged — update cache but suppress redundant callbacks.
		if oldTopology != nil && atappTopologyEqual(oldTopology, payload.Value) {
			return
		}
	} else if eventType == EtcdWatchEventDelete {
		id := parseTopologyIDFromPath(payload.Key)
		if id != 0 {
			m.mu.Lock()
			delete(m.topologyInfoSet, id)
			m.mu.Unlock()
		}
	}

	var topologyStorage *TopologyStorage
	if payload.Value != nil {
		topologyStorage = &TopologyStorage{
			Info: payload.Value,
			Version: EtcdDataVersion{
				CreateRevision: payload.CreateRevision,
				ModRevision:    payload.ModRevision,
				Version:        payload.Version,
			},
		}
	}

	sender := &TopologyWatcherSender{Module: m, Topology: topologyStorage, Action: eventType}
	version := &EtcdDataVersion{
		CreateRevision: payload.CreateRevision,
		ModRevision:    payload.ModRevision,
		Version:        payload.Version,
	}

	m.cbMu.Lock()
	topologyInfoCallbacks := make([]TopologyInfoEventCallback, 0, len(m.topoEventCbs))
	for _, fn := range m.topoEventCbs {
		topologyInfoCallbacks = append(topologyInfoCallbacks, fn)
	}
	topologyWatcherCallbacks := make([]TopologyWatcherListCallback, len(m.topoWatcherCbs))
	copy(topologyWatcherCallbacks, m.topoWatcherCbs)
	m.cbMu.Unlock()

	for _, fn := range topologyInfoCallbacks {
		fn(eventType, payload.Value, version)
	}
	for _, fn := range topologyWatcherCallbacks {
		fn(sender)
	}
}

func toWatchNodePayload(payload any) (*modulev2.WatchNodePayload, bool) {
	switch value := payload.(type) {
	case modulev2.WatchNodePayload:
		copyValue := value
		return &copyValue, true
	case *modulev2.WatchNodePayload:
		return value, value != nil
	default:
		return nil, false
	}
}

func toWatchTopologyPayload(payload any) (*modulev2.WatchTopologyPayload, bool) {
	switch value := payload.(type) {
	case modulev2.WatchTopologyPayload:
		copyValue := value
		return &copyValue, true
	case *modulev2.WatchTopologyPayload:
		return value, value != nil
	default:
		return nil, false
	}
}

func toWatchTopologySnapshotLoadedPayload(payload any) (*modulev2.WatchTopologySnapshotLoadedPayload, bool) {
	switch value := payload.(type) {
	case modulev2.WatchTopologySnapshotLoadedPayload:
		copyValue := value
		return &copyValue, true
	case *modulev2.WatchTopologySnapshotLoadedPayload:
		return value, value != nil
	default:
		return nil, false
	}
}

func toWatchSnapshotLoadedPayload(payload any) (*modulev2.WatchSnapshotLoadedPayload, bool) {
	switch value := payload.(type) {
	case modulev2.WatchSnapshotLoadedPayload:
		copyValue := value
		return &copyValue, true
	case *modulev2.WatchSnapshotLoadedPayload:
		return value, value != nil
	default:
		return nil, false
	}
}

func parseTopologyIDFromPath(path string) uint64 {
	if path == "" {
		return 0
	}
	lastSlash := strings.LastIndex(path, "/")
	nameWithID := path
	if lastSlash >= 0 && lastSlash+1 < len(path) {
		nameWithID = path[lastSlash+1:]
	}
	lastDash := strings.LastIndex(nameWithID, "-")
	if lastDash < 0 || lastDash+1 >= len(nameWithID) {
		return 0
	}
	id, err := strconv.ParseUint(nameWithID[lastDash+1:], 10, 64)
	if err != nil {
		return 0
	}
	return id
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
