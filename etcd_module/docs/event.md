# etcd 事件系统工作机制详解

本文档说明 `service_discovery` 项目中 **事件驱动架构**（`pkg/events`）的设计、实现和完整工作流程。

---

## 核心概念

C++ libatapp 使用**函数指针钩子**（`on_node_discovery_up`、`add_on_load_snapshot` 等）实现事件回调——每种事件对应一个具名函数指针，一次只能绑定一个回调。

Go 实现将其扩展为**事件总线**模型：

- **EventManager**：通用事件总线，支持多订阅者，按 `EventType` 分发，线程安全
- **CallbackList**：轻量级回调列表，直接对应 C++ 单函数指针钩子
- **EventStream**：基于 channel 的事件流，适合 goroutine 消费（C++ 无对应，Go 原生扩展）

这三套机制并存、互补，共同构成完整的事件系统。

---

## 整体架构（与 C++ libatapp 完全对齐）

```
EtcdCluster（对应 C++ etcd_cluster）
  │
  ├─ EventManager（通用事件总线）
  │    ├─ Subscribe(types, callback) → EventCallbackHandle
  │    ├─ Unsubscribe(handle)
  │    ├─ Publish(event)             → 分发给所有匹配的订阅者
  │    └─ Close()
  │
  ├─ CallbackList（专用轻量回调，对应 C++ 具名钩子）
  │    ├─ nodeEventCallbacks          ← 对应 on_node_discovery_up/down/update
  │    ├─ snapshotLoadingCallbacks    ← 对应 add_on_load_snapshot
  │    └─ snapshotLoadedCallbacks     ← 对应 add_on_snapshot_loaded
  │
  └─ EventStream（Go 原生扩展，channel-based）
       ├─ NewEventStream(ctx, bufferSize, eventTypes)
       ├─ Events() <-chan events.Event
       └─ Close()

上层 API（pkg/module/events_api.go，对应 C++ etcd_module::add_on_*）
  └─ EtcdModule → cluster.AddOnNodeEvent / AddOnSnapshotLoading / AddOnSnapshotLoaded
```

---

## 事件类型详解

### 节点事件

| EventType | String() | 触发条件 | C++ 对应 |
|-----------|---------|---------|---------|
| `EventTypeNodeUp` | `"node_up"` | etcd PUT，节点首次出现（`existed=false`） | `on_node_discovery_up` 钩子 |
| `EventTypeNodeDown` | `"node_down"` | etcd DELETE，节点消失 | `on_node_discovery_down` 钩子 |
| `EventTypeNodeUpdate` | `"node_update"` | etcd PUT，节点已存在（`existed=true`），值变化 | libatapp 扩展（原版无此钩子） |

### 集群生命周期事件

| EventType | String() | 触发条件 | C++ 对应 |
|-----------|---------|---------|---------|
| `EventTypeClusterUp` | `"cluster_up"` | `startInternal()` 成功 | `etcd_cluster::start()` 完成 |
| `EventTypeClusterDown` | `"cluster_down"` | `stopInternal()` 调用 | `etcd_cluster::stop()` / 析构 |
| `EventTypeClusterChange` | `"cluster_change"` | 集群状态机转换（连接/就绪/断开） | `conf_.on_etcd_event` 状态变化 |

### Watch 连接事件

| EventType | String() | 触发条件 | C++ 对应 |
|-----------|---------|---------|---------|
| `EventTypeWatchConnected` | `"watch_connected"` | watch stream 首次建立 | `create_request_watcher` 成功 |
| `EventTypeWatchDisconnected` | `"watch_disconnected"` | watch stream 断开，等待重连 | watch stream 断开，重试等待 |
| `EventTypeWatchReconnected` | `"watch_reconnected"` | watch stream 重连成功 | watch stream 重连后恢复 |

### Snapshot 事件

| EventType | String() | 触发条件 | C++ 对应 |
|-----------|---------|---------|---------|
| `EventTypeSnapshotLoading` | `"snapshot_loading"` | 全量 List 开始前 | `add_on_load_snapshot` 回调 |
| `EventTypeSnapshotLoaded` | `"snapshot_loaded"` | 全量 List 完成后 | `add_on_snapshot_loaded` 回调 |

---

## 完整事件发布流程图

### 节点事件路径

```
etcd Watch 事件（PUT / DELETE）
  │
  └─ EtcdWatcher.dispatchEvent(EtcdWatchEvent)
       │
       └─ cluster.makeWatcherEventHandler()
            │
            └─ discovery.EtcdDiscoverySet.HandleWatcherEvent()
                 │
                 └─ SetEventPublisher 回调（makeDiscoveryEventPublisher）
                      │
                      ├─ PUT + existed=false → NewNodeUpEvent(node)
                      │         ├─ em.Publish(event)               ← EventManager（所有订阅者）
                      │         └─ nodeEventCallbacks.Publish(event) ← CallbackList（直接钩子）
                      │
                      ├─ PUT + existed=true  → NewNodeUpdateEvent(node)
                      │         ├─ em.Publish(event)
                      │         └─ nodeEventCallbacks.Publish(event)
                      │
                      └─ DELETE → NewNodeDownEvent(nodeID, nodeName)
                                ├─ em.Publish(event)
                                └─ nodeEventCallbacks.Publish(event)
```

### Watch 连接状态路径

```
EtcdWatcher 连接状态变化
  │
  └─ cluster.makeWatcherConnectionHandler()
       │
       ├─ Connected    → em.Publish(NewWatchConnectedEvent(revision))
       ├─ Disconnected → em.Publish(NewWatchDisconnectedEvent())
       └─ Reconnected  → em.Publish(NewWatchReconnectedEvent(revision))
```

### Snapshot 路径

```
EtcdWatcher.loadSnapshot()
  │
  ├─ snapshotLoadingHandler() → makeWatcherSnapshotLoadingHandler()
  │       ├─ em.Publish(NewSnapshotLoadingEvent())
  │       └─ snapshotLoadingCallbacks.Publish(event)
  │
  └─ snapshotHandler() → makeWatcherSnapshotHandler()
          ├─ em.Publish(NewSnapshotLoadedEvent(nodeCount, revision))
          │       └─ Metadata["node_count"] = nodeCount
          ├─ snapshotLoadedCallbacks.Publish(event)
          └─ markReadyIfNeeded()
                  └─ em.Publish(NewClusterStateChangeEvent(ClusterStateReady, revision))
```

### 集群生命周期路径

```
startInternal()
  ├─ em.Publish(NewClusterUpEvent())
  └─ em.Publish(NewClusterStateChangeEvent(ClusterStateConnected, 0))

stopInternal()
  ├─ em.Publish(NewClusterDownEvent())
  └─ em.Publish(NewClusterStateChangeEvent(ClusterStateDisconnected, 0))
```

---

## 三种订阅方式

### 一、EventManager.Subscribe()

通用事件总线，支持多订阅者，按 `EventType` 分发，内置 `sync.RWMutex` 线程安全。

**源码位置**：`pkg/events/events.go`

```go
// 订阅多种事件类型
handle := em.Subscribe(
    []events.EventType{events.EventTypeNodeUp, events.EventTypeNodeDown},
    func(event *events.Event) {
        // event.Type / event.Node / event.NodeID / event.NodeName
    },
)

// 取消订阅
em.Unsubscribe(handle)

// 按类型批量取消
em.UnsubscribeByType([]events.EventType{events.EventTypeNodeUp})

// 关闭整个事件管理器（清理所有订阅）
em.Close()
```

- `Publish()` 内部复制 `Event` 结构体（`eventCopy := *event`），避免并发竞争
- 在 `done` channel 关闭后，`Publish()` 立即停止派发

### 二、CallbackList

轻量级回调列表，无锁，**不**线程安全，由调用方保证并发安全。直接对应 C++ 各具名函数指针钩子。

**源码位置**：`pkg/events/events.go`

```go
// CallbackList 的使用方式（cluster 内部）：
list := events.NewCallbackList()

handle := list.Add(func(event *events.Event) {
    // 处理节点事件 / snapshot 事件
})

list.Remove(handle)

// 发布给列表中所有回调
list.Publish(event)
```

cluster 内部使用的三个具名列表（对应 C++ 钩子）：

| 列表名 | 对应 C++ 钩子 | 用途 |
|--------|-------------|------|
| `nodeEventCallbacks` | `on_node_discovery_up/down` | 节点发现事件 |
| `snapshotLoadingCallbacks` | `add_on_load_snapshot` | 快照开始加载 |
| `snapshotLoadedCallbacks` | `add_on_snapshot_loaded` | 快照加载完成 |

### 三、EventStream（channel-based）

Go 原生扩展，将 `EventManager.Subscribe()` 封装为 channel，适合 goroutine 消费。

**源码位置**：`pkg/cluster/stream.go`

```go
// 创建事件流（bufferSize 默认 100，<=0 时自动设为 100）
stream := cluster.NewEventStream(ctx, 100,
    []events.EventType{events.EventTypeNodeUp, events.EventTypeNodeDown},
)
defer stream.Close() // 取消订阅 + close(ch)

// 在 goroutine 中消费
for event := range stream.Events() {
    // 处理 event
}
```

关键行为：
- channel 已满时**丢弃事件**（非阻塞 send），不阻塞发布者
- `ctx.Done()` 触发时自动调用 `stream.Close()`
- `Close()` 幂等，多次调用安全

---

## 各事件发布点详解

| 事件类型 | 触发条件 | 发布位置 | 附加字段 |
|---------|---------|---------|---------|
| `EventTypeNodeUp` | etcd PUT，新节点 | `cluster/cluster.go: makeDiscoveryEventPublisher` | `Node`、`NodeID`、`NodeName` |
| `EventTypeNodeDown` | etcd DELETE | `cluster/cluster.go: makeDiscoveryEventPublisher` | `NodeID`、`NodeName` |
| `EventTypeNodeUpdate` | etcd PUT，已存在 | `cluster/cluster.go: makeDiscoveryEventPublisher` | `Node`、`NodeID`、`NodeName` |
| `EventTypeClusterUp` | 集群启动完成 | `cluster/cluster.go: startInternal` | 无 |
| `EventTypeClusterDown` | 集群停止 | `cluster/cluster.go: stopInternal` | 无 |
| `EventTypeClusterChange` | 状态机转换 | `cluster/cluster.go: startInternal / stopInternal / markReadyIfNeeded` | `ClusterState`、`Revision` |
| `EventTypeWatchConnected` | watch stream 建立 | `cluster/cluster.go: makeWatcherConnectionHandler` | `Revision` |
| `EventTypeWatchDisconnected` | watch stream 断开 | `cluster/cluster.go: makeWatcherConnectionHandler` | 无 |
| `EventTypeWatchReconnected` | watch stream 重连 | `cluster/cluster.go: makeWatcherConnectionHandler` | `Revision` |
| `EventTypeSnapshotLoading` | 全量 List 开始前 | `cluster/cluster.go: makeWatcherSnapshotLoadingHandler` | 无 |
| `EventTypeSnapshotLoaded` | 全量 List 完成后 | `cluster/cluster.go: makeWatcherSnapshotHandler` | `Revision`、`Metadata["node_count"]` |

---

## 两套回调机制并存

| 维度 | EventManager | CallbackList |
|------|-------------|-------------|
| 多订阅者 | ✅ 支持（`map[EventCallbackHandle]*eventSubscription`） | ✅ 支持（`map[EventCallbackHandle]EventCallback`） |
| 取消订阅 | `Unsubscribe(handle)` / `UnsubscribeByType(types)` | `Remove(handle)` |
| 线程安全 | ✅ `sync.RWMutex` | ❌ 无锁，由调用方保证 |
| 事件复制 | ✅ `eventCopy := *event`（防竞争） | ❌ 直接传指针 |
| 关闭机制 | `Close()` — 关闭 `done` channel，停止派发 | 无 `Close`，列表由所有者生命周期管理 |
| 使用场景 | 通用事件总线，外部订阅者 | 内部具名钩子，对应 C++ `add_on_*` |
| C++ 对应 | 多个 `add_on_*` 函数指针（Go 扩展为多订阅者） | 单个函数指针钩子（一对一） |

---

## 上层 API（module 层）

`pkg/module/events_api.go` 提供与 C++ `etcd_module` 对齐的门面 API：

| Go module API | cluster 方法 | C++ 对应 |
|--------------|------------|---------|
| `EtcdModule.AddOnNodeDiscoveryEvent(cb)` | `cluster.AddOnNodeEvent(cb)` | `etcd_module::add_on_node_discovery_event()` |
| `EtcdModule.RemoveOnNodeEvent(handle)` | `cluster.RemoveOnNodeEvent(handle)` | `etcd_module::remove_on_node_event()` |
| `EtcdModule.AddOnLoadSnapshot(cb)` | `cluster.AddOnSnapshotLoading(cb)` | `etcd_module::add_on_load_snapshot()` |
| `EtcdModule.RemoveOnLoadSnapshot(handle)` | `cluster.RemoveOnSnapshotLoading(handle)` | `etcd_module::remove_on_load_snapshot()` |
| `EtcdModule.AddOnSnapshotLoaded(cb)` | `cluster.AddOnSnapshotLoaded(cb)` | `etcd_module::add_on_snapshot_loaded()` |
| `EtcdModule.RemoveOnSnapshotLoaded(handle)` | `cluster.RemoveOnSnapshotLoaded(handle)` | `etcd_module::remove_on_snapshot_loaded()` |

---

## C++ libatapp 对应关系

| Go 实现 | C++ 对应 | 说明 |
|---------|---------|------|
| `EventTypeNodeUp` | `on_node_discovery_up` 钩子 | etcd PUT，节点首次出现 |
| `EventTypeNodeDown` | `on_node_discovery_down` 钩子 | etcd DELETE，节点消失 |
| `EventTypeNodeUpdate` | （libatapp 扩展，原版无此钩子） | etcd PUT，节点值变化 |
| `EventTypeClusterUp` | `etcd_cluster::start()` 完成 | 集群启动 |
| `EventTypeClusterDown` | `etcd_cluster::stop()` / 析构 | 集群停止 |
| `EventTypeClusterChange` | `conf_.on_etcd_event` 状态变化 | 状态机转换 |
| `EventTypeWatchConnected` | watch stream 建立（`create_request_watcher`） | 首次连接 |
| `EventTypeWatchDisconnected` | watch stream 断开，重试等待 | 连接断开 |
| `EventTypeWatchReconnected` | watch stream 重连成功 | 重连 |
| `EventTypeSnapshotLoading` | `add_on_load_snapshot` 回调 | 全量 List 开始前 |
| `EventTypeSnapshotLoaded` | `add_on_snapshot_loaded` 回调 | 全量 List 完成后 |
| `EventManager.Subscribe()` | 多个 `add_on_*` 函数指针 | C++ 单回调 → Go 多订阅者 |
| `CallbackList` | C++ 单个函数指针钩子 | 直接对应各 `add_on_*` |
| `ClusterState` 枚举 | `etcd_cluster` 内部状态枚举 | Disconnected/Connecting/Connected/Ready |
| `EventStream` | 无直接对应 | Go 原生扩展：channel-based 消费 |

---

## 架构差异（Go vs C++ libatapp）

| 维度 | Go 实现 | C++ libatapp |
|------|--------|-------------|
| 派发机制 | 事件总线（`EventManager.Subscribe/Publish`） | 函数指针（`on_node_*` 钩子） |
| 多订阅者 | ✅ 支持（`map` of callbacks） | ❌ 一个事件一个钩子 |
| 取消订阅 | handle-based `Unsubscribe` | 重新赋值 `nil` |
| Channel 消费 | ✅ `EventStream`（Go 扩展） | ❌ 无 |
| 线程安全 | ✅ `sync.RWMutex`（EventManager） | 由调用者保证 |
| 事件复制 | ✅ `eventCopy := *event`，防并发竞争 | 直接调用，无拷贝 |

---

## 相关源码文件

| 文件 | 说明 |
|------|------|
| `pkg/events/events.go` | `EventType`、`Event`、`ClusterState`、`EventManager`、`CallbackList`、所有 `New*Event()` 构造函数 |
| `pkg/cluster/stream.go` | `EventStream`：channel-based 事件流，封装 `EventManager.Subscribe` |
| `pkg/cluster/cluster.go` | 所有事件发布点：`makeDiscoveryEventPublisher`、`makeWatcherConnectionHandler`、`makeWatcherSnapshotLoadingHandler`、`makeWatcherSnapshotHandler`、`startInternal`、`stopInternal`、`markReadyIfNeeded` |
| `pkg/module/events_api.go` | 上层门面 API：`AddOnNodeDiscoveryEvent`、`AddOnLoadSnapshot`、`AddOnSnapshotLoaded` 等 |
| `pkg/watcher/watcher.go` | 事件触发源：`loadSnapshot`、`updateConnectionState`、`handleCanceledWatch` |
| `pkg/discovery/discovery.go` | `EtcdDiscoverySet.HandleWatcherEvent`：解析 etcd 事件，调用 `SetEventPublisher` 回调 |
