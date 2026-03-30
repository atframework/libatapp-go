package module

import (
	"context"
	"fmt"
	"sync"

	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/cluster"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"

	"google.golang.org/protobuf/proto"
)

type EtcdModule struct {
	core     etcdModuleCore
	config   etcdModuleConfig
	runtime  etcdModuleRuntime
	topology etcdModuleTopology
	mu       sync.RWMutex
}

type etcdModuleCore struct {
	cluster         *cluster.EtcdCluster
	globalDiscovery *discovery.EtcdDiscoverySet
	logger          *log.Logger
}

type etcdModuleConfig struct {
	etcd       *atframe_protocol.AtappEtcd
	path       string
	customData string
	enabled    bool
}

func (c *etcdModuleConfig) isEnabled() bool {
	if c == nil {
		return false
	}
	return c.enabled
}

func (c *etcdModuleConfig) setEnabled(enabled bool) {
	if c == nil {
		return
	}
	c.enabled = enabled
}

func (c *etcdModuleConfig) getEtcdClone() *atframe_protocol.AtappEtcd {
	if c == nil || c.etcd == nil {
		return nil
	}
	cfg, _ := proto.Clone(c.etcd).(*atframe_protocol.AtappEtcd)
	return cfg
}

func (c *etcdModuleConfig) setEtcd(cfg *atframe_protocol.AtappEtcd) {
	if c == nil {
		return
	}
	c.etcd = cfg
}

func (c *etcdModuleConfig) getPath() string {
	if c == nil {
		return ""
	}
	return c.path
}

func (c *etcdModuleConfig) setPath(path string) {
	if c == nil {
		return
	}
	c.path = path
}

func (c *etcdModuleConfig) getCustomData() string {
	if c == nil {
		return ""
	}
	return c.customData
}

func (c *etcdModuleConfig) setCustomData(customData string) {
	if c == nil {
		return
	}
	c.customData = customData
}

type etcdModuleRuntime struct {
	lastDiscoveryHeader *atframe_protocol.EtcdResponseHeader
	snapshotReady       bool
	snapshotHookSet     bool
}

func (r *etcdModuleRuntime) setLastDiscoveryHeader(header *atframe_protocol.EtcdResponseHeader) {
	if r == nil || header == nil {
		return
	}
	r.lastDiscoveryHeader = proto.Clone(header).(*atframe_protocol.EtcdResponseHeader)
}

func (r *etcdModuleRuntime) getLastDiscoveryHeaderClone() *atframe_protocol.EtcdResponseHeader {
	if r == nil || r.lastDiscoveryHeader == nil {
		return nil
	}
	return proto.Clone(r.lastDiscoveryHeader).(*atframe_protocol.EtcdResponseHeader)
}

func (r *etcdModuleRuntime) setSnapshotReady(ready bool) {
	if r == nil {
		return
	}
	r.snapshotReady = ready
}

func (r *etcdModuleRuntime) isSnapshotReady() bool {
	if r == nil {
		return false
	}
	return r.snapshotReady
}

func (r *etcdModuleRuntime) isSnapshotHookSet() bool {
	if r == nil {
		return false
	}
	return r.snapshotHookSet
}

func (r *etcdModuleRuntime) markSnapshotHookSet() {
	if r == nil {
		return
	}
	r.snapshotHookSet = true
}

type etcdModuleTopology struct {
	lastHeader       *atframe_protocol.EtcdResponseHeader
	snapshotReady    bool
	onLoadSnapshot   *events.CallbackList
	onSnapshotLoaded *events.CallbackList
	onInfoEvent      *events.CallbackList
	infoSet          map[string]*TopologyInfo
	keepalivePath    string
	keepaliveDirty   bool
	pendingKeepalive *atframe_protocol.AtappTopologyInfo
	lastKeepalive    *atframe_protocol.AtappTopologyInfo
	keepaliveSource  func() *atframe_protocol.AtappTopologyInfo
}

func (m *EtcdModule) clusterCtx() *cluster.EtcdCluster {
	if m == nil {
		return nil
	}
	return m.core.cluster
}

func (m *EtcdModule) globalDiscoverySet() *discovery.EtcdDiscoverySet {
	if m == nil {
		return nil
	}
	return m.core.globalDiscovery
}

func (m *EtcdModule) moduleLogger() *log.Logger {
	if m == nil {
		return nil
	}
	return m.core.logger
}

func (m *EtcdModule) setLastEtcdEventHeader(header *atframe_protocol.EtcdResponseHeader) {
	if m == nil || header == nil {
		return
	}
	m.mu.Lock()
	m.runtime.setLastDiscoveryHeader(header)
	m.mu.Unlock()
}

func (m *EtcdModule) setLastEtcdEventTopologyHeader(header *atframe_protocol.EtcdResponseHeader) {
	if m == nil || header == nil {
		return
	}
	m.mu.Lock()
	m.topology.setLastHeader(header)
	m.mu.Unlock()
}

func newEtcdModuleWithCluster(c *cluster.EtcdCluster, logger *log.Logger) (*EtcdModule, error) {
	return NewEtcdModuleWithCluster(c, logger)
}

// NewEtcdModuleWithCluster 创建并返回EtcdModule。
func NewEtcdModuleWithCluster(c *cluster.EtcdCluster, logger *log.Logger) (*EtcdModule, error) {
	if c == nil {
		return nil, fmt.Errorf("cluster is nil")
	}
	if logger == nil {
		logger = log.Default()
	}
	ds, err := discovery.NewEtcdDiscoverySet("", logger)
	if err != nil {
		return nil, err
	}
	return &EtcdModule{
		core: etcdModuleCore{
			cluster:         c,
			globalDiscovery: ds,
			logger:          logger,
		},
		config: etcdModuleConfig{
			enabled: true,
		},
		topology: etcdModuleTopology{
			onLoadSnapshot:   events.NewCallbackList(),
			onSnapshotLoaded: events.NewCallbackList(),
			onInfoEvent:      events.NewCallbackList(),
			infoSet:          map[string]*TopologyInfo{},
		},
	}, nil
}

// IsEtcdEnabled 判断是否满足EtcdEnabled条件。
func (m *EtcdModule) IsEtcdEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.isEnabled()
}

// EnableEtcd 实现。
func (m *EtcdModule) EnableEtcd() {
	m.mu.Lock()
	m.config.setEnabled(true)
	m.mu.Unlock()
}

// DisableEtcd 实现。
func (m *EtcdModule) DisableEtcd() {
	m.mu.Lock()
	m.config.setEnabled(false)
	m.mu.Unlock()
}

// GetConfigure 获取Configure。
func (m *EtcdModule) GetConfigure() *atframe_protocol.AtappEtcd {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.getEtcdClone()
}

// GetConfigurePath 获取ConfigurePath。
func (m *EtcdModule) GetConfigurePath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.getPath()
}

// SetConfigurePath 设置ConfigurePath。
func (m *EtcdModule) SetConfigurePath(path string) {
	m.mu.Lock()
	m.config.setPath(path)
	m.mu.Unlock()
}

// GetConfCustomData 获取ConfCustomData。
func (m *EtcdModule) GetConfCustomData() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.getCustomData()
}

// SetConfCustomData 设置ConfCustomData。
func (m *EtcdModule) SetConfCustomData(v string) {
	m.mu.Lock()
	m.config.setCustomData(v)
	m.mu.Unlock()
}

// GetRawEtcdCtx 获取RawEtcdCtx。
func (m *EtcdModule) GetRawEtcdCtx() *cluster.EtcdCluster {
	return m.clusterCtx()
}

// GetGlobalDiscovery 获取GlobalDiscovery。
func (m *EtcdModule) GetGlobalDiscovery() (*discovery.EtcdDiscoverySet, error) {
	cl := m.clusterCtx()
	if cl == nil {
		return nil, fmt.Errorf("cluster is nil")
	}
	return cl.GetDiscoverySet()
}

// HasSnapshot 判断是否存在Snapshot。
func (m *EtcdModule) HasSnapshot() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtime.isSnapshotReady()
}

// Init 实现。
func (m *EtcdModule) Init(ctx context.Context) error {
	if !m.IsEtcdEnabled() {
		return nil
	}
	m.mu.Lock()
	if !m.runtime.isSnapshotHookSet() {
		cl := m.clusterCtx()
		if cl != nil {
			cl.AddOnSnapshotLoaded(func(event *events.Event) {
				m.mu.Lock()
				m.runtime.setSnapshotReady(true)
				m.mu.Unlock()
			})
		}
		m.runtime.markSnapshotHookSet()
	}
	m.mu.Unlock()

	cl := m.clusterCtx()
	if cl == nil {
		return fmt.Errorf("cluster is nil")
	}

	if err := cl.RegisterDiscoverySet(m.globalDiscoverySet()); err != nil {
		if logger := m.moduleLogger(); logger != nil {
			logger.Warn("Failed to register global discovery set", "error", err)
		}
	}
	if err := cl.Start(ctx); err != nil {
		return err
	}
	m.syncTopologyKeepaliveFromSource()
	if err := m.flushTopologyKeepalive(ctx); err != nil {
		if logger := m.moduleLogger(); logger != nil {
			logger.Warn("Failed to flush topology keepalive on init", "error", err)
		}
	}
	return nil
}

// Stop 停止Stop。
func (m *EtcdModule) Stop(ctx context.Context) error {
	cl := m.clusterCtx()
	if cl == nil {
		return fmt.Errorf("cluster is nil")
	}
	return cl.Stop(ctx)
}

func (m *EtcdModule) Reset(ctx context.Context) error {
	cl := m.clusterCtx()
	if cl == nil {
		return fmt.Errorf("cluster is nil")
	}
	return cl.Stop(ctx)
}

// Reload 实现。
func (m *EtcdModule) Reload(ctx context.Context, cfg *atframe_protocol.AtappEtcd) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	cl := m.clusterCtx()
	if cl == nil {
		return fmt.Errorf("cluster is nil")
	}
	if cl.IsAvailable() {
		return fmt.Errorf("cluster already started")
	}
	m.mu.Lock()
	m.config.setEtcd(cfg)
	m.mu.Unlock()
	cl.ApplyEtcdConfig(cfg)
	return nil
}

// Tick 实现。
func (m *EtcdModule) Tick(ctx context.Context) error {
	if !m.IsEtcdEnabled() {
		return nil
	}
	m.syncTopologyKeepaliveFromSource()
	if err := m.flushTopologyKeepalive(ctx); err != nil {
		return err
	}
	cl := m.clusterCtx()
	if cl == nil {
		return fmt.Errorf("cluster is nil")
	}
	return cl.Tick(ctx)
}

func (m *EtcdModule) triggerMaybeUpdateKeepalives(reason string) {
	if m == nil {
		return
	}
	cl := m.clusterCtx()
	if cl == nil {
		return
	}
	if err := cl.TriggerMaybeUpdateRegistrations(context.Background()); err != nil {
		if logger := m.moduleLogger(); logger != nil {
			logger.Debug("skip maybe-update keepalives", "reason", reason, "error", err)
		}
	}
}

// SetMaybeUpdateKeepaliveValue 设置MaybeUpdateKeepaliveValue。
func (m *EtcdModule) SetMaybeUpdateKeepaliveValue() {
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveValue")
}

// SetMaybeUpdateKeepaliveArea 设置MaybeUpdateKeepaliveArea。
func (m *EtcdModule) SetMaybeUpdateKeepaliveArea() {
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveArea")
}

// SetMaybeUpdateKeepaliveMetadata 设置MaybeUpdateKeepaliveMetadata。
func (m *EtcdModule) SetMaybeUpdateKeepaliveMetadata() {
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveMetadata")
}

// --- Keepalive 更新标记 ---

func (m *EtcdModule) SetMaybeUpdateKeepaliveTopologyValue() {
	m.syncTopologyKeepaliveFromSource()
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveTopologyValue")
}
func (m *EtcdModule) SetMaybeUpdateKeepaliveDiscoveryValue() {
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveDiscoveryValue")
}
func (m *EtcdModule) SetMaybeUpdateKeepaliveDiscoveryArea() {
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveDiscoveryArea")
}
func (m *EtcdModule) SetMaybeUpdateKeepaliveDiscoveryMetadata() {
	m.triggerMaybeUpdateKeepalives("SetMaybeUpdateKeepaliveDiscoveryMetadata")
}
