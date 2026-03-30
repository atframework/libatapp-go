package libatapp

import (
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	moduleimpl "github.com/atframework/libatapp-go/etcd_module/module"
	"github.com/atframework/libatapp-go/etcd_module/registration"
	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"
)

// ============ EtcdModule 接口 ============

// EtcdAppModuleImpl defines the interface for the etcd service discovery module.
// It extends AppModuleImpl with etcd-specific functionality.
type EtcdAppModuleImpl interface {
	AppModuleImpl
	EtcdModuleImpl
}

// ============ 服务发现数据和事件相关定义 ============

// EtcdDiscoveryAction represents the action type for discovery node events.
type EtcdDiscoveryAction = moduleimpl.DiscoveryAction

const (
	EtcdDiscoveryActionUnknown EtcdDiscoveryAction = moduleimpl.DiscoveryActionUnknown
	EtcdDiscoveryActionPut     EtcdDiscoveryAction = moduleimpl.DiscoveryActionPut
	EtcdDiscoveryActionDelete  EtcdDiscoveryAction = moduleimpl.DiscoveryActionDelete
)

// EtcdWatchEvent represents the event type for etcd watch operations.
type EtcdWatchEvent = moduleimpl.TopologyAction

const (
	EtcdWatchEventUnknown EtcdWatchEvent = moduleimpl.TopologyActionUnknown
	EtcdWatchEventPut     EtcdWatchEvent = moduleimpl.TopologyActionPut
	EtcdWatchEventDelete  EtcdWatchEvent = moduleimpl.TopologyActionDelete
)

// EtcdResponseHeader reuses the generated proto type.
type EtcdResponseHeader = atframe_protocol.EtcdResponseHeader

// EtcdDataVersion represents version information for etcd data.
type EtcdDataVersion struct {
	CreateRevision int64
	ModRevision    int64
	Version        int64
}

// EtcdDiscoveryNodeVersion represents version info for a discovery node.
type EtcdDiscoveryNodeVersion struct {
	CreateRevision int64
	ModRevision    int64
	Version        int64
}

// EtcdDiscoveryNode reuses discovery node model from etcd_module package.
type EtcdDiscoveryNode = discovery.DiscoveryNode

// EtcdDiscoverySet reuses discovery set model from etcd_module package.
type EtcdDiscoverySet = discovery.EtcdDiscoverySet

// EtcdRegistration is the canonical registration actor model.
type EtcdRegistration = registration.EtcdRegistration

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

// TopologyInfo represents topology data with an associated action.
type TopologyInfo struct {
	Storage TopologyStorage
	Action  EtcdWatchEvent
}

// TopologyList represents a list of topology information entries.
type TopologyList struct {
	Topologies []TopologyInfo
}

// ============ Watcher 回调参数 ============

// DiscoveryWatcherSender carries context for a discovery watcher callback invocation.
type DiscoveryWatcherSender struct {
	Module     EtcdModuleImpl
	EtcdHeader *EtcdResponseHeader
	Node       *NodeInfo
}

// TopologyWatcherSender carries context for a topology watcher callback invocation.
type TopologyWatcherSender struct {
	Module     EtcdModuleImpl
	EtcdHeader *EtcdResponseHeader
	Topology   *TopologyInfo
}

// ============ 回调类型 ============

type DiscoveryWatcherListCallback func(*DiscoveryWatcherSender)
type TopologyWatcherListCallback func(*TopologyWatcherSender)
type DiscoverySnapshotEventCallback func(EtcdModuleImpl)
type TopologySnapshotEventCallback func(EtcdModuleImpl)
type NodeEventCallback func(EtcdDiscoveryAction, *EtcdDiscoveryNode)
type TopologyInfoEventCallback func(EtcdWatchEvent, *atframe_protocol.AtappTopologyInfo, *EtcdDataVersion)

// EventCallbackHandle reuses callback handle type from etcd_module events package.
type EventCallbackHandle = events.EventCallbackHandle

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

	// --- Etcd 事件头 ---

	GetLastEtcdEventTopologyHeader() *EtcdResponseHeader
	GetLastEtcdEventDiscoveryHeader() *EtcdResponseHeader

	// --- Registration Actor ---

	AddRegistrationActor(val *string, nodePath string) *EtcdRegistration
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

func CreateEtcdModule(app AppModuleImpl) EtcdAppModuleImpl {
	if app == nil {
		return nil
	}
	return newEtcdModuleAdapter(app.GetApp())
}
