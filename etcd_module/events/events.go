// Package events provides event-driven architecture for service discovery.
// It implements event publishing, subscribing, and callback management.
package events

import (
	"sync"
	"time"

	"github.com/atframework/libatapp-go/etcd_module/discovery"
)

// EventType 定义EventType类型。
type EventType int

const (
	// EventTypeNodeUp is fired when a new node is discovered.
	EventTypeNodeUp EventType = iota
	// EventTypeNodeDown is fired when a node is removed.
	EventTypeNodeDown
	// EventTypeNodeUpdate is fired when a node's metadata is updated.
	EventTypeNodeUpdate
	// EventTypeClusterUp is fired when the cluster becomes available.
	EventTypeClusterUp
	// EventTypeClusterDown is fired when the cluster becomes unavailable.
	EventTypeClusterDown
	// EventTypeClusterChange is fired when cluster state changes significantly.
	EventTypeClusterChange
	// EventTypeWatchConnected is fired when watcher establishes connection.
	EventTypeWatchConnected
	// EventTypeWatchDisconnected is fired when watcher loses connection.
	EventTypeWatchDisconnected
	// EventTypeWatchReconnected is fired when watcher reconnects.
	EventTypeWatchReconnected
	// EventTypeSnapshotLoaded is fired when initial snapshot is loaded.
	EventTypeSnapshotLoaded
	// EventTypeSnapshotLoading is fired before snapshot load.
	EventTypeSnapshotLoading
)

const (
	// EventTypeLeaseGranted is fired when a cluster lease is first granted.
	EventTypeLeaseGranted EventType = iota + 100
	// EventTypeLeaseExpired is fired when the cluster lease keepalive channel closes (etcd TTL natural expiry).
	EventTypeLeaseExpired
	// EventTypeLeaseReleased is fired when the cluster lease is actively cancelled or revoked by this node.
	EventTypeLeaseReleased
)

// String 实现。
func (t EventType) String() string {
	switch t {
	case EventTypeNodeUp:
		return "node_up"
	case EventTypeNodeDown:
		return "node_down"
	case EventTypeNodeUpdate:
		return "node_update"
	case EventTypeClusterUp:
		return "cluster_up"
	case EventTypeClusterDown:
		return "cluster_down"
	case EventTypeClusterChange:
		return "cluster_change"
	case EventTypeWatchConnected:
		return "watch_connected"
	case EventTypeWatchDisconnected:
		return "watch_disconnected"
	case EventTypeWatchReconnected:
		return "watch_reconnected"
	case EventTypeSnapshotLoaded:
		return "snapshot_loaded"
	case EventTypeSnapshotLoading:
		return "snapshot_loading"
	case EventTypeLeaseGranted:
		return "lease_granted"
	case EventTypeLeaseExpired:
		return "lease_expired"
	case EventTypeLeaseReleased:
		return "lease_released"
	default:
		return "unknown"
	}
}

// Event 定义Event类型。
type Event struct {
	Type         EventType
	Timestamp    time.Time
	Node         *discovery.DiscoveryNode
	NodeID       uint64
	NodeName     string
	ClusterState ClusterState
	Revision     int64
	Metadata     map[string]interface{}
}

// ClusterState 定义ClusterState类型。
type ClusterState int

const (
	// ClusterStateUnknown indicates the cluster state is unknown.
	ClusterStateUnknown ClusterState = iota
	// ClusterStateDisconnected indicates disconnected from etcd.
	ClusterStateDisconnected
	// ClusterStateConnecting indicates connecting to etcd.
	ClusterStateConnecting
	// ClusterStateConnected indicates connected to etcd.
	ClusterStateConnected
	// ClusterStateReady indicates ready to serve requests.
	ClusterStateReady
)

// String 实现。
func (s ClusterState) String() string {
	switch s {
	case ClusterStateUnknown:
		return "unknown"
	case ClusterStateDisconnected:
		return "disconnected"
	case ClusterStateConnecting:
		return "connecting"
	case ClusterStateConnected:
		return "connected"
	case ClusterStateReady:
		return "ready"
	default:
		return "invalid"
	}
}

// EventCallback 定义EventCallback回调函数类型。
type EventCallback func(event *Event)

// EventCallbackHandle 定义EventCallbackHandle类型。
type EventCallbackHandle int

type eventCallbackDispatcher struct {
	done <-chan struct{}
}

func (d eventCallbackDispatcher) Dispatch(callbacks []EventCallback, event *Event) {
	if event == nil {
		return
	}
	for _, callback := range callbacks {
		if d.done != nil {
			select {
			case <-d.done:
				return
			default:
			}
		}
		callback(event)
	}
}

func snapshotCallbackMap[K comparable](source map[K]EventCallback) []EventCallback {
	callbacks := make([]EventCallback, 0, len(source))
	for _, cb := range source {
		callbacks = append(callbacks, cb)
	}
	return callbacks
}

type CallbackList struct {
	mu         sync.RWMutex
	nextHandle EventCallbackHandle
	callbacks  map[EventCallbackHandle]EventCallback
}

func NewCallbackList() *CallbackList {
	return &CallbackList{
		callbacks: make(map[EventCallbackHandle]EventCallback),
	}
}

func (c *CallbackList) Add(callback EventCallback) EventCallbackHandle {
	if callback == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextHandle++
	handle := c.nextHandle
	c.callbacks[handle] = callback
	return handle
}

func (c *CallbackList) Remove(handle EventCallbackHandle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.callbacks, handle)
}

func (c *CallbackList) Publish(event *Event) {
	eventCallbackDispatcher{}.Dispatch(c.snapshotCallbacks(), event)
}

func (c *CallbackList) snapshotCallbacks() []EventCallback {
	c.mu.RLock()
	callbacks := snapshotCallbackMap(c.callbacks)
	c.mu.RUnlock()
	return callbacks
}

// EventManager 定义EventManager管理器结构。
type EventManager interface {
	// Subscribe registers a callback for the given event types.
	Subscribe(types []EventType, callback EventCallback) EventCallbackHandle
	// Unsubscribe removes a callback using its handle.
	Unsubscribe(handle EventCallbackHandle) error
	// UnsubscribeByType removes all callbacks for the given event types.
	UnsubscribeByType(types []EventType) error
	// Publish publishes an event to all registered callbacks.
	Publish(event *Event)
	// GetSubscriberCount returns the number of subscribers for a specific event type.
	GetSubscriberCount(eventType EventType) int
	// Close cleans up all subscriptions.
	Close()
}

// eventSubscription 定义eventSubscription类型。
type eventSubscription struct {
	types    []EventType
	callback EventCallback
}

// eventManager 定义eventManager管理器结构。
type eventManager struct {
	mu            sync.RWMutex
	subscriptions map[EventCallbackHandle]*eventSubscription
	nextHandle    EventCallbackHandle
	callbacks     map[EventType][]EventCallbackHandle
	done          chan struct{}
	closed        bool
}

// NewEventManager 创建并返回EventManager。
func NewEventManager() EventManager {
	return &eventManager{
		subscriptions: make(map[EventCallbackHandle]*eventSubscription),
		nextHandle:    1,
		callbacks:     make(map[EventType][]EventCallbackHandle),
		done:          make(chan struct{}),
	}
}

// Subscribe 实现。
func (m *eventManager) Subscribe(types []EventType, callback EventCallback) EventCallbackHandle {
	if callback == nil {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 关闭后不允许新的订阅
	if m.closed {
		return 0
	}

	handle := m.nextHandle
	m.nextHandle++

	m.subscriptions[handle] = &eventSubscription{
		types:    types,
		callback: callback,
	}

	for _, eventType := range types {
		m.callbacks[eventType] = append(m.callbacks[eventType], handle)
	}

	return handle
}

// Unsubscribe 实现。
func (m *eventManager) Unsubscribe(handle EventCallbackHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, ok := m.subscriptions[handle]
	if !ok {
		return nil
	}

	for _, eventType := range sub.types {
		handles := m.callbacks[eventType]
		for i, h := range handles {
			if h == handle {
				m.callbacks[eventType] = append(handles[:i], handles[i+1:]...)
				break
			}
		}
	}

	delete(m.subscriptions, handle)
	return nil
}

// UnsubscribeByType 实现。
func (m *eventManager) UnsubscribeByType(types []EventType) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	handlesToRemove := make(map[EventCallbackHandle]bool)

	for _, eventType := range types {
		for _, handle := range m.callbacks[eventType] {
			handlesToRemove[handle] = true
		}
		m.callbacks[eventType] = nil
	}

	for handle := range handlesToRemove {
		sub, ok := m.subscriptions[handle]
		if ok {
			for _, eventType := range sub.types {
				handles := m.callbacks[eventType]
				for i := len(handles) - 1; i >= 0; i-- {
					if handles[i] == handle {
						handles = append(handles[:i], handles[i+1:]...)
					}
				}
				m.callbacks[eventType] = handles
			}
		}
		delete(m.subscriptions, handle)
	}

	return nil
}

// Publish 实现。
func (m *eventManager) Publish(event *Event) {
	if event == nil {
		return
	}

	// 深拷贝 Event 以保护原始 event 和指针字段（Node、Metadata）不被 callback 修改
	eventCopy := m.deepCopyEvent(event)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	callbacks := m.snapshotCallbacksForTypeLocked(event.Type)
	m.mu.Unlock()
	eventCallbackDispatcher{done: m.done}.Dispatch(callbacks, &eventCopy)
}

func (m *eventManager) snapshotCallbacksForType(eventType EventType) []EventCallback {
	m.mu.RLock()
	callbacks := m.snapshotCallbacksForTypeLocked(eventType)
	m.mu.RUnlock()
	return callbacks
}

func (m *eventManager) snapshotCallbacksForTypeLocked(eventType EventType) []EventCallback {
	handles := m.callbacks[eventType]
	if len(handles) == 0 {
		return nil
	}

	callbacks := make([]EventCallback, 0, len(handles))
	for _, handle := range handles {
		if sub, ok := m.subscriptions[handle]; ok {
			callbacks = append(callbacks, sub.callback)
		}
	}

	return callbacks
}

// GetSubscriberCount 获取SubscriberCount。
func (m *eventManager) GetSubscriberCount(eventType EventType) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.callbacks[eventType])
}

// deepCopyEvent 创建 Event 的深拷贝，包括 Metadata 和 Node 指针指向的数据。
func (m *eventManager) deepCopyEvent(event *Event) Event {
	eventCopy := *event
	// 深拷贝 Metadata map
	if event.Metadata != nil {
		eventCopy.Metadata = make(map[string]interface{})
		for k, v := range event.Metadata {
			eventCopy.Metadata[k] = v
		}
	}
	// Node 指针保留原始引用（DiscoveryNode 本身应该是不可变的或有其他保护机制）
	return eventCopy
}

// Close 关闭模块并释放底层资源。
func (m *eventManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	close(m.done)
	// Close 采用非阻塞语义，避免 callback 内调用 Close 时出现自死锁。
	// done 已关闭，dispatcher 会在当前回调返回后停止后续回调分发。
	m.subscriptions = make(map[EventCallbackHandle]*eventSubscription)
	m.callbacks = make(map[EventType][]EventCallbackHandle)
	m.mu.Unlock()
}

// NewNodeUpEvent 创建并返回Event。
func NewNodeUpEvent(node *discovery.DiscoveryNode) *Event {
	event := &Event{
		Type:      EventTypeNodeUp,
		Timestamp: time.Now(),
		Node:      node,
		Metadata:  make(map[string]interface{}),
	}
	if node != nil && node.Info != nil {
		event.NodeID = node.Info.Id
		event.NodeName = node.Info.Name
	}
	return event
}

// NewNodeDownEvent 创建并返回Event。
func NewNodeDownEvent(nodeID uint64, nodeName string) *Event {
	return &Event{
		Type:      EventTypeNodeDown,
		Timestamp: time.Now(),
		NodeID:    nodeID,
		NodeName:  nodeName,
		Metadata:  make(map[string]interface{}),
	}
}

// NewNodeUpdateEvent 创建并返回Event。
func NewNodeUpdateEvent(node *discovery.DiscoveryNode) *Event {
	event := &Event{
		Type:      EventTypeNodeUpdate,
		Timestamp: time.Now(),
		Node:      node,
		Metadata:  make(map[string]interface{}),
	}
	if node != nil && node.Info != nil {
		event.NodeID = node.Info.Id
		event.NodeName = node.Info.Name
	}
	return event
}

// NewClusterStateChangeEvent 创建并返回Event。
func NewClusterStateChangeEvent(state ClusterState, revision int64) *Event {
	return &Event{
		Type:         EventTypeClusterChange,
		Timestamp:    time.Now(),
		ClusterState: state,
		Revision:     revision,
		Metadata:     make(map[string]interface{}),
	}
}

// NewClusterUpEvent 创建并返回Event。
func NewClusterUpEvent() *Event {
	return &Event{
		Type:      EventTypeClusterUp,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
}

// NewClusterDownEvent 创建并返回Event。
func NewClusterDownEvent() *Event {
	return &Event{
		Type:      EventTypeClusterDown,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
}

// NewWatchConnectedEvent 创建并返回Event。
func NewWatchConnectedEvent(revision int64) *Event {
	return &Event{
		Type:      EventTypeWatchConnected,
		Timestamp: time.Now(),
		Revision:  revision,
		Metadata:  make(map[string]interface{}),
	}
}

// NewWatchDisconnectedEvent 创建并返回Event。
func NewWatchDisconnectedEvent() *Event {
	return &Event{
		Type:      EventTypeWatchDisconnected,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
}

// NewWatchReconnectedEvent 创建并返回Event。
func NewWatchReconnectedEvent(revision int64) *Event {
	return &Event{
		Type:      EventTypeWatchReconnected,
		Timestamp: time.Now(),
		Revision:  revision,
		Metadata:  make(map[string]interface{}),
	}
}

// NewSnapshotLoadedEvent 创建并返回Event。
func NewSnapshotLoadedEvent(nodeCount int, revision int64) *Event {
	return &Event{
		Type:      EventTypeSnapshotLoaded,
		Timestamp: time.Now(),
		Revision:  revision,
		Metadata: map[string]interface{}{
			"node_count": nodeCount,
		},
	}
}

// NewSnapshotLoadingEvent 创建并返回Event。
func NewSnapshotLoadingEvent() *Event {
	return &Event{
		Type:      EventTypeSnapshotLoading,
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
}

// NewLeaseGrantedEvent 创建并返回 lease granted Event。
// leaseID is int64 matching clientv3.LeaseID; ttl is the requested lease TTL in seconds.
func NewLeaseGrantedEvent(leaseID int64, ttl int64) *Event {
	return &Event{
		Type:      EventTypeLeaseGranted,
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"lease_id": leaseID,
			"ttl":      ttl,
		},
	}
}

// NewLeaseExpiredEvent 创建并返回 lease expired Event（etcd TTL 自然耗尽）。
func NewLeaseExpiredEvent(leaseID int64) *Event {
	return &Event{
		Type:      EventTypeLeaseExpired,
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"lease_id": leaseID,
		},
	}
}

// NewLeaseReleasedEvent 创建并返回 lease released Event（本节点主动 cancel/revoke）。
func NewLeaseReleasedEvent(leaseID int64) *Event {
	return &Event{
		Type:      EventTypeLeaseReleased,
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"lease_id": leaseID,
		},
	}
}
