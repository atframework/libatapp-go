package module

import (
	"context"
	"errors"
	"fmt"
	log "log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/atframework/libatapp-go/etcd_module/client"
	"github.com/atframework/libatapp-go/etcd_module/cluster"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	"github.com/atframework/libatapp-go/etcd_module/internal/codec"
	"github.com/atframework/libatapp-go/etcd_module/registration"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

const defaultRegisterTTLSeconds int64 = 16

var ErrCPPModuleSemanticMismatch = errors.New("cpp etcd_module semantic mismatch")

// NodeEventCallback 定义NodeEventCallback回调函数类型。
type NodeEventCallback func(action DiscoveryAction, node *discovery.DiscoveryNode)

// SnapshotEventCallback 定义SnapshotEventCallback回调函数类型。
type SnapshotEventCallback func(mod *EtcdModule)

// NewEtcdModuleFromConfig 创建并返回EtcdModule。
func NewEtcdModuleFromConfig(cfg *pb.AtappEtcd, logger *log.Logger) (*EtcdModule, error) {
	if cfg == nil {
		return nil, fmt.Errorf("etcd config is nil")
	}

	if len(cfg.GetHosts()) == 0 {
		return nil, fmt.Errorf("etcd hosts are empty")
	}

	conn, err := client.NewEtcdClusterClient(cfg, logger)
	if err != nil {
		return nil, err
	}

	cl, err := cluster.NewEtcdCluster(conn, logger)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	mod, err := newEtcdModuleWithCluster(cl, logger)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if err := mod.Reload(context.Background(), cfg); err != nil {
		_ = mod.Close(context.Background())
		return nil, err
	}

	if cfg.GetPath() != "" {
		mod.SetConfigurePath(cfg.GetPath())
	}
	if !cfg.GetEnable() {
		mod.DisableEtcd()
	}

	// 配置已通过 Reload -> ApplyEtcdConfig 应用，无需单独调用 SetLeaseTTL
	return mod, nil
}

// Close 关闭模块并释放底层资源。
func (m *EtcdModule) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}

	stopErr := m.Stop(ctx)
	if errors.Is(stopErr, cluster.ErrClusterAlreadyStopped) {
		stopErr = nil
	}

	var closeErr error
	if cl := m.clusterCtx(); cl != nil {
		closeErr = cl.CloseEtcdClient()
	}

	return errors.Join(stopErr, closeErr)
}

// Name 返回模块名称。
func (m *EtcdModule) Name() string {
	return "etcd_module"
}

// Timeout 返回模块超时时间。
func (m *EtcdModule) Timeout() int {
	return 0
}

// GetLastEtcdEventHeader 获取最近一次 etcd 事件头信息副本。
func (m *EtcdModule) GetLastEtcdEventHeader() *pb.EtcdResponseHeader {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtime.getLastDiscoveryHeaderClone()
}

// GetSharedCurlMultiContext 返回共享 Curl Multi 上下文（Go 版本不支持该能力）。
func (m *EtcdModule) GetSharedCurlMultiContext() (interface{}, error) {
	return nil, fmt.Errorf("%w: curl multi context is C++-specific and not exposed in Go", ErrCPPModuleSemanticMismatch)
}

// AddRegistrationActorByValue 按 JSON 值注册 Keepalive Actor。
func (m *EtcdModule) AddRegistrationActorByValue(ctx context.Context, val string, nodePath string, ttl int64) (*registration.EtcdRegistration, error) {
	return m.AddRegistrationByValue(ctx, val, nodePath, ttl)
}

// AddRegistrationByValue 按 JSON 值注册 Registration Actor。
func (m *EtcdModule) AddRegistrationByValue(ctx context.Context, val string, nodePath string, ttl int64) (*registration.EtcdRegistration, error) {
	info := &pb.AtappDiscovery{}
	if err := codec.UnmarshalDiscoveryFromJSON([]byte(val), info); err != nil {
		return nil, fmt.Errorf("unmarshal discovery json failed: %w", err)
	}
	return m.AddRegistration(ctx, info, nodePath, normalizeTTL(ttl))
}

// AddRegistrationActorByValueDefaultTTL 按默认 TTL 通过 JSON 值注册 Keepalive Actor。
func (m *EtcdModule) AddRegistrationActorByValueDefaultTTL(ctx context.Context, val string, nodePath string) (*registration.EtcdRegistration, error) {
	return m.AddRegistrationByValueDefaultTTL(ctx, val, nodePath)
}

// AddRegistrationByValueDefaultTTL 按默认 TTL 通过 JSON 值注册 Registration Actor。
func (m *EtcdModule) AddRegistrationByValueDefaultTTL(ctx context.Context, val string, nodePath string) (*registration.EtcdRegistration, error) {
	return m.AddRegistrationByValue(ctx, val, nodePath, defaultRegisterTTLSeconds)
}

// AddOnNodeDiscoveryEventCompat 添加兼容 C++ 风格的节点发现事件回调。
func (m *EtcdModule) AddOnNodeDiscoveryEventCompat(fn NodeEventCallback) events.EventCallbackHandle {
	if fn == nil {
		return 0
	}
	return m.AddOnNodeDiscoveryEvent(func(event *events.Event) {
		if event == nil {
			return
		}
		fn(discoveryActionFromEventType(event.Type), event.Node)
	})
}

// AddOnLoadSnapshotCompat 添加兼容 C++ 风格的快照加载前回调。
func (m *EtcdModule) AddOnLoadSnapshotCompat(fn SnapshotEventCallback) events.EventCallbackHandle {
	if fn == nil {
		return 0
	}
	return m.AddOnLoadSnapshot(func(event *events.Event) {
		_ = event
		fn(m)
	})
}

// AddOnSnapshotLoadedCompat 添加兼容 C++ 风格的快照加载完成回调。
func (m *EtcdModule) AddOnSnapshotLoadedCompat(fn SnapshotEventCallback) events.EventCallbackHandle {
	if fn == nil {
		return 0
	}
	return m.AddOnSnapshotLoaded(func(event *events.Event) {
		_ = event
		fn(m)
	})
}

// AddWatcherByCustomPathAndGet 按自定义路径添加 Watcher 并返回对应实例。
func (m *EtcdModule) AddWatcherByCustomPathAndGet(ctx context.Context, path string) (*watcher.EtcdWatcher, error) {
	if err := m.AddWatcherByCustomPath(ctx, path); err != nil {
		return nil, err
	}
	wm := m.GetRawEtcdCtx().GetWatcherManager()
	if wm == nil {
		return nil, fmt.Errorf("watcher manager is nil")
	}
	w, ok := wm.GetWatcher(path)
	if !ok {
		return nil, fmt.Errorf("watcher not found for path: %s", path)
	}
	return w, nil
}

// AddWatcherByIDCallback 按 ID 维度添加带回调的 Watcher。
func (m *EtcdModule) AddWatcherByIDCallback(ctx context.Context, fn WatcherSenderListCallback) error {
	return m.AddWatcherByIDWithSender(ctx, fn)
}

// AddWatcherByNameCallback 按 Name 维度添加带回调的 Watcher。
func (m *EtcdModule) AddWatcherByNameCallback(ctx context.Context, fn WatcherSenderListCallback) error {
	return m.AddWatcherByNameWithSender(ctx, fn)
}

// AddWatcherByTypeIDCallback 按 TypeID 维度添加带回调的 Watcher。
func (m *EtcdModule) AddWatcherByTypeIDCallback(ctx context.Context, typeID uint64, fn WatcherSenderOneCallback) error {
	return m.AddWatcherByTypeIDWithSender(ctx, typeID, fn)
}

// AddWatcherByTypeNameCallback 按 TypeName 维度添加带回调的 Watcher。
func (m *EtcdModule) AddWatcherByTypeNameCallback(ctx context.Context, typeName string, fn WatcherSenderOneCallback) error {
	return m.AddWatcherByTypeNameWithSender(ctx, typeName, fn)
}

// AddWatcherByTagCallback 按 Tag 维度添加带回调的 Watcher。
func (m *EtcdModule) AddWatcherByTagCallback(ctx context.Context, tag string, fn WatcherSenderOneCallback) error {
	return m.AddWatcherByTagWithSender(ctx, tag, fn)
}

// AddWatcherByCustomPathCallback 按自定义路径添加带回调的 Watcher。
func (m *EtcdModule) AddWatcherByCustomPathCallback(ctx context.Context, path string, fn WatcherSenderOneCallback) (*watcher.EtcdWatcher, error) {
	return m.AddWatcherByCustomPathWithSender(ctx, path, fn)
}

// AddOnNodeDiscoveryEvent 添加节点发现事件回调。
func (m *EtcdModule) AddOnNodeDiscoveryEvent(callback events.EventCallback) events.EventCallbackHandle {
	return m.clusterCtx().AddOnNodeEvent(callback)
}

// RemoveOnNodeEvent 移除节点事件回调。
func (m *EtcdModule) RemoveOnNodeEvent(handle events.EventCallbackHandle) {
	m.clusterCtx().RemoveOnNodeEvent(handle)
}

// AddOnLoadSnapshot 添加快照加载前回调。
func (m *EtcdModule) AddOnLoadSnapshot(callback events.EventCallback) events.EventCallbackHandle {
	return m.clusterCtx().AddOnSnapshotLoading(callback)
}

// RemoveOnLoadSnapshot 移除快照加载前回调。
func (m *EtcdModule) RemoveOnLoadSnapshot(handle events.EventCallbackHandle) {
	m.clusterCtx().RemoveOnSnapshotLoading(handle)
}

// AddOnSnapshotLoaded 添加快照加载完成回调。
func (m *EtcdModule) AddOnSnapshotLoaded(callback events.EventCallback) events.EventCallbackHandle {
	return m.clusterCtx().AddOnSnapshotLoaded(callback)
}

// RemoveOnSnapshotLoaded 移除快照加载完成回调。
func (m *EtcdModule) RemoveOnSnapshotLoaded(handle events.EventCallbackHandle) {
	m.clusterCtx().RemoveOnSnapshotLoaded(handle)
}

// AddRegistrationActor 注册 Keepalive Actor 并返回对应实例。
func (m *EtcdModule) AddRegistrationActor(ctx context.Context, info *pb.AtappDiscovery, path string, ttl int64) (*registration.EtcdRegistration, error) {
	return m.AddRegistration(ctx, info, path, ttl)
}

// AddRegistration 注册 Registration Actor 并返回对应实例。
func (m *EtcdModule) AddRegistration(ctx context.Context, info *pb.AtappDiscovery, path string, ttl int64) (*registration.EtcdRegistration, error) {
	if err := m.clusterCtx().RegisterService(ctx, info, path, ttl); err != nil {
		return nil, err
	}
	km := m.clusterCtx().GetRegistrationManager()
	if km == nil {
		return nil, fmt.Errorf("registration manager is not initialized")
	}
	k, ok := km.GetRegistration(path)
	if !ok {
		return nil, fmt.Errorf("registration registered but not found in manager: %s", path)
	}
	return k, nil
}

// RemoveRegistrationActor 移除指定路径的 Keepalive Actor。
func (m *EtcdModule) RemoveRegistrationActor(ctx context.Context, path string) error {
	return m.RemoveRegistration(ctx, path)
}

// RemoveRegistration 移除指定路径的 Registration Actor。
func (m *EtcdModule) RemoveRegistration(ctx context.Context, path string) error {
	return m.clusterCtx().UnregisterService(ctx, path)
}

// AddWatcherByID 按 ID 维度添加 Watcher。
func (m *EtcdModule) AddWatcherByID(ctx context.Context) error {
	return m.clusterCtx().AddWatcher(ctx, m.GetByIDWatcherPath(), nil)
}

// AddWatcherByTypeID 按 TypeID 维度添加 Watcher。
func (m *EtcdModule) AddWatcherByTypeID(ctx context.Context, typeID uint64) error {
	return m.clusterCtx().AddWatcher(ctx, m.GetByTypeIDWatcherPath(typeID), nil)
}

// AddWatcherByTypeName 按 TypeName 维度添加 Watcher。
func (m *EtcdModule) AddWatcherByTypeName(ctx context.Context, typeName string) error {
	return m.clusterCtx().AddWatcher(ctx, m.GetByTypeNameWatcherPath(typeName), nil)
}

// AddWatcherByName 按 Name 维度添加 Watcher。
func (m *EtcdModule) AddWatcherByName(ctx context.Context) error {
	return m.clusterCtx().AddWatcher(ctx, m.GetByNameWatcherPath(), nil)
}

// AddWatcherByTag 按 Tag 维度添加 Watcher。
func (m *EtcdModule) AddWatcherByTag(ctx context.Context, tag string) error {
	return m.clusterCtx().AddWatcher(ctx, m.GetByTagWatcherPath(tag), nil)
}

// AddWatcherByCustomPath 按自定义路径添加 Watcher。
func (m *EtcdModule) AddWatcherByCustomPath(ctx context.Context, path string) error {
	return m.clusterCtx().AddWatcher(ctx, path, nil)
}

// RegisterByID 按 ID 维度注册服务并创建 Keepalive。
func (m *EtcdModule) RegisterByID(ctx context.Context, info *pb.AtappDiscovery, ttl int64) (*registration.EtcdRegistration, error) {
	normalized, err := normalizeServiceInfo(info)
	if err != nil {
		return nil, err
	}
	return m.AddRegistrationActor(ctx, normalized, m.GetByIDPath(normalized), normalizeTTL(ttl))
}

// RegisterByTypeID 按 TypeID 维度注册服务并创建 Keepalive。
func (m *EtcdModule) RegisterByTypeID(ctx context.Context, info *pb.AtappDiscovery, ttl int64) (*registration.EtcdRegistration, error) {
	normalized, err := normalizeServiceInfo(info)
	if err != nil {
		return nil, err
	}
	return m.AddRegistrationActor(ctx, normalized, m.GetByTypeIDPath(normalized), normalizeTTL(ttl))
}

// RegisterByTypeName 按 TypeName 维度注册服务并创建 Keepalive。
func (m *EtcdModule) RegisterByTypeName(ctx context.Context, info *pb.AtappDiscovery, ttl int64) (*registration.EtcdRegistration, error) {
	normalized, err := normalizeServiceInfo(info)
	if err != nil {
		return nil, err
	}
	return m.AddRegistrationActor(ctx, normalized, m.GetByTypeNamePath(normalized), normalizeTTL(ttl))
}

// RegisterByName 按 Name 维度注册服务并创建 Keepalive。
func (m *EtcdModule) RegisterByName(ctx context.Context, info *pb.AtappDiscovery, ttl int64) (*registration.EtcdRegistration, error) {
	normalized, err := normalizeServiceInfo(info)
	if err != nil {
		return nil, err
	}
	return m.AddRegistrationActor(ctx, normalized, m.GetByNamePath(normalized), normalizeTTL(ttl))
}

// RegisterByTag 按 Tag 维度注册服务并创建 Keepalive。
func (m *EtcdModule) RegisterByTag(ctx context.Context, info *pb.AtappDiscovery, tag string, ttl int64) (*registration.EtcdRegistration, error) {
	normalized, err := normalizeServiceInfo(info)
	if err != nil {
		return nil, err
	}
	return m.AddRegistrationActor(ctx, normalized, m.GetByTagPath(normalized, tag), normalizeTTL(ttl))
}

// UnregisterByID 按 ID 维度注销服务。
func (m *EtcdModule) UnregisterByID(ctx context.Context, info *pb.AtappDiscovery) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	return m.RemoveRegistrationActor(ctx, m.GetByIDPath(info))
}

// UnregisterByTypeID 按 TypeID 维度注销服务。
func (m *EtcdModule) UnregisterByTypeID(ctx context.Context, info *pb.AtappDiscovery) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	return m.RemoveRegistrationActor(ctx, m.GetByTypeIDPath(info))
}

// UnregisterByTypeName 按 TypeName 维度注销服务。
func (m *EtcdModule) UnregisterByTypeName(ctx context.Context, info *pb.AtappDiscovery) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	return m.RemoveRegistrationActor(ctx, m.GetByTypeNamePath(info))
}

// UnregisterByName 按 Name 维度注销服务。
func (m *EtcdModule) UnregisterByName(ctx context.Context, info *pb.AtappDiscovery) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	return m.RemoveRegistrationActor(ctx, m.GetByNamePath(info))
}

// UnregisterByTag 按 Tag 维度注销服务。
func (m *EtcdModule) UnregisterByTag(ctx context.Context, info *pb.AtappDiscovery, tag string) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	return m.RemoveRegistrationActor(ctx, m.GetByTagPath(info, tag))
}

func normalizeTTL(ttl int64) int64 {
	if ttl > 0 {
		return ttl
	}
	return defaultRegisterTTLSeconds
}

func normalizeServiceInfo(info *pb.AtappDiscovery) (*pb.AtappDiscovery, error) {
	if info == nil {
		return nil, fmt.Errorf("service info is nil")
	}

	cloned, _ := proto.Clone(info).(*pb.AtappDiscovery)
	if cloned == nil {
		return nil, fmt.Errorf("failed to clone service info")
	}

	if cloned.Identity == "" {
		if cloned.Name == "" && cloned.Id == 0 {
			return nil, fmt.Errorf("identity and service name/id are empty")
		}
		if cloned.Name == "" {
			cloned.Identity = formatUint(cloned.Id)
		} else {
			cloned.Identity = fmt.Sprintf("%s-%d", cloned.Name, cloned.Id)
		}
	}

	return cloned, nil
}
