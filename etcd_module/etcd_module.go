package libatapp_etcd_module

import (
	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"

	libatapp_types "github.com/atframework/libatapp-go/types"
)

// ============ EtcdModule 接口 ============

// EtcdAppModuleImpl defines the interface for the etcd service discovery module.
// It extends AppModuleImpl with etcd-specific functionality.
type EtcdAppModuleImpl interface {
	libatapp_types.AppModuleImpl
	EtcdModuleImpl
}

// ============ 服务发现数据和事件相关定义 ============

// EtcdDiscoveryAction represents the action type for discovery node events.
type EtcdDiscoveryAction int

const (
	EtcdDiscoveryActionUnknown EtcdDiscoveryAction = 0
	EtcdDiscoveryActionPut     EtcdDiscoveryAction = 1
	EtcdDiscoveryActionDelete  EtcdDiscoveryAction = 2
)

// EtcdWatchEvent represents the event type for etcd watch operations.
type EtcdWatchEvent int

const (
	EtcdWatchEventUnknown EtcdWatchEvent = 0
	EtcdWatchEventPut     EtcdWatchEvent = 1
	EtcdWatchEventDelete  EtcdWatchEvent = 2
)

// EtcdResponseHeader reuses the generated proto type.
type EtcdResponseHeader = atframe_protocol.EtcdResponseHeader

// EtcdDataVersion represents version information for etcd data.
type EtcdDataVersion = modulev2.DataVersion

// EtcdDiscoveryNodeVersion represents version info for a discovery node.
type EtcdDiscoveryNodeVersion = modulev2.DataVersion

// EtcdDiscoveryNode reuses v2 discovery node read-model.
type EtcdDiscoveryNode = modulev2.DiscoveryNode

// EtcdDiscoverySet reuses v2 discovery set read-model.
type EtcdDiscoverySet = modulev2.DiscoverySetSnapshot

// EtcdRegistration is a legacy path token returned by registration APIs.
// Keepalive ownership is in v2 module internals.
type EtcdRegistration struct {
	path string
	// Err holds an error from the most recent registration attempt.
	// nil means the registration succeeded.
	// Use errors.Is(reg.GetError(), modulev2.ErrCheckerConflict) to detect
	// ownership conflicts.
	Err error
}

// GetPath returns the registration key path.
func (r *EtcdRegistration) GetPath() string {
	if r == nil {
		return ""
	}
	return r.path
}

// GetError returns any error recorded during registration.
// nil indicates success.  A non-nil error means the registration write was
// either rejected (e.g. ErrCheckerConflict) or failed for another reason.
func (r *EtcdRegistration) GetError() error {
	if r == nil {
		return nil
	}
	return r.Err
}

// NodeInfo represents discovery node information with an associated action.
type NodeInfo struct {
	NodeDiscovery *atframe_protocol.AtappDiscovery
	Action        EtcdDiscoveryAction
}

// NodeList represents a list of node information entries.
type NodeList struct {
	Nodes []NodeInfo
}

// ============ 拓扑数据和事件相关定义 ============

// TopologyStorage stores topology information with version.
type TopologyStorage struct {
	Info    *atframe_protocol.AtappTopologyInfo
	Version EtcdDataVersion
}

// DiscoveryNodeStorage stores discovery node information with version.
type DiscoveryNodeStorage struct {
	Info    *atframe_protocol.AtappDiscovery
	Version EtcdDataVersion
}

// ============ Watcher 回调参数 ============

// DiscoveryWatcherSender carries context for a discovery watcher callback invocation.
type DiscoveryWatcherSender struct {
	Module EtcdModuleImpl
	Node   *NodeInfo
}

// TopologyWatcherSender carries context for a topology watcher callback invocation.
type TopologyWatcherSender struct {
	Module   EtcdModuleImpl
	Topology *TopologyStorage
	Action   EtcdWatchEvent
}

// ============ 回调类型 ============

type (
	DiscoveryWatcherListCallback   func(*DiscoveryWatcherSender)
	TopologyWatcherListCallback    func(*TopologyWatcherSender)
	DiscoverySnapshotEventCallback func(EtcdModuleImpl)
	TopologySnapshotEventCallback  func(EtcdModuleImpl)
	NodeEventCallback              func(EtcdDiscoveryAction, *EtcdDiscoveryNode)
	TopologyInfoEventCallback      func(EtcdWatchEvent, *atframe_protocol.AtappTopologyInfo, *EtcdDataVersion)
)

// EventCallbackHandle identifies a registered callback in this module.
type EventCallbackHandle int64

type EtcdModuleImpl interface {
	// Reset clears all internal state.
	Reset()

	// --- 配置 ---

	GetConfCustomData() string
	SetConfCustomData(v string)
	GetConfigure() *atframe_protocol.AtappEtcd
	GetConfigurePath() string

	// --- Etcd 开关 ---

	IsEtcdEnabled() bool
	EnableEtcd()
	DisableEtcd()

	// --- Keepalive 更新标记 ---

	SetMaybeUpdateKeepaliveTopologyValue()
	SetMaybeUpdateKeepaliveDiscoveryValue()
	SetMaybeUpdateKeepaliveDiscoveryArea()
	SetMaybeUpdateKeepaliveDiscoveryMetadata()

	// --- 路径访问 ---

	GetDiscoveryByIdPath() string
	GetDiscoveryByNamePath() string
	GetTopologyPath() string
	GetDiscoveryByIdWatcherPath() string
	GetDiscoveryByNameWatcherPath() string
	GetTopologyWatcherPath() string

	// --- Watcher 注册 ---

	AddDiscoveryWatcherById(fn DiscoveryWatcherListCallback) error
	AddDiscoveryWatcherByName(fn DiscoveryWatcherListCallback) error
	AddTopologyWatcher(fn TopologyWatcherListCallback) error

	// --- Registration Actor ---

	AddRegistrationDiscoveryActor(val *atframe_protocol.AtappDiscovery, nodePath string) *EtcdRegistration
	AddRegistrationTopologyActor(val *atframe_protocol.AtappTopologyInfo, nodePath string) *EtcdRegistration
	RemoveRegistrationActor(registration *EtcdRegistration) bool

	// --- 节点发现事件 ---

	AddOnNodeDiscoveryEvent(fn NodeEventCallback) EventCallbackHandle
	RemoveOnNodeEvent(handle EventCallbackHandle)

	// --- 拓扑信息事件 ---

	AddOnTopologyInfoEvent(fn TopologyInfoEventCallback) EventCallbackHandle
	RemoveOnTopologyInfoEvent(handle EventCallbackHandle)

	// --- 全局 Discovery ---

	GetGlobalDiscovery() *EtcdDiscoverySet

	// --- 拓扑信息集合 ---

	GetTopologyInfoSet() map[uint64]*TopologyStorage

	// --- Discovery 节点集合 ---

	GetDiscoveryNodeSet() map[string]*DiscoveryNodeStorage

	// --- Discovery 快照 ---

	HasDiscoverySnapshot() bool
	AddOnLoadDiscoverySnapshot(fn DiscoverySnapshotEventCallback) EventCallbackHandle
	RemoveOnLoadDiscoverySnapshot(handle EventCallbackHandle)
	AddOnDiscoverySnapshotLoaded(fn DiscoverySnapshotEventCallback) EventCallbackHandle
	RemoveOnDiscoverySnapshotLoaded(handle EventCallbackHandle)

	// --- Topology 快照 ---

	HasTopologySnapshot() bool
	AddOnLoadTopologySnapshot(fn TopologySnapshotEventCallback) EventCallbackHandle
	RemoveOnLoadTopologySnapshot(handle EventCallbackHandle)
	AddOnTopologySnapshotLoaded(fn TopologySnapshotEventCallback) EventCallbackHandle
	RemoveOnTopologySnapshotLoaded(handle EventCallbackHandle)
}

func CreateEtcdModule(app libatapp_types.AppModuleImpl) EtcdAppModuleImpl {
	if app == nil {
		return nil
	}
	return newEtcdModuleAdapter(app.GetApp())
}
