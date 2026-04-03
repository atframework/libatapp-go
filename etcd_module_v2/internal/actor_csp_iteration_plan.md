# etcd_module 并发模型演进设计

> 版本: v2.0 | 日期: 2026-03-30
>
> **v1.0 → v2.0 策略转变**：v1.0 以 `etcd_module/cluster` 为施工对象，采用 Phase 1-5
> 渐进补丁路径修复并发问题。v2.0 转为以 `etcd_module_v2` 为正式目标包，在其中建立清晰的
> 四层架构，从设计上绕过 cluster 层的历史遗留问题，而非逐一修补。
> cluster 层仅保留一个必要的独立 bug fix（P2 kaCtx），不再驱动架构方向。

---

## 零、当前结构总览（讨论基线）

本节描述的是“当前我们确认采用的结构定义”，用于后续实现对齐。

### 0.1 分层结构

```
etcd_module_v2/
├── module.go                             // 对外接口层（Facade）
├── types.go                              // 对外类型
└── internal/
    ├── orchestrator/                     // 业务编排层
    │   ├── lease_actor.go                // LeaseActor（租约状态机，写模型驱动）
    │   ├── registration_actor.go         // RegistrationActor（注册写入与重放）
    │   ├── watch_actor.go                // WatchActor（watch 流）
    │   └── projection_actor.go           // ProjectionActor（统一快照发布）
    ├── runtime/                          // 并发基础层
    │   ├── actor.go                      // actor mailbox 与 run loop
    │   ├── event_bus.go                  // 领域事件总线
    │   └── lifecycle.go                  // goroutine 生命周期管理
    └── snapshot/                         // 快照子结构定义
        ├── registration_snapshot.go      // Registration 子视图结构
        └── export_snapshot.go            // ExportSnapshot 聚合结构

etcd_module/client/                       // etcd 连接层（Grant/KeepAlive/Put/Get/Watch）
go.etcd.io/etcd/client/v3                // clientv3 SDK 层（底层连接与协议客户端）
```

### 0.2 组件职责

1. `LeaseActor`：管理 lease 生命周期（grant/keepalive/rebuild），发布 lease 事件。
2. `RegistrationActor`：根据配置驱动注册写入（bypath/byname/byid/topology），在 lease 变化后重放。
3. `WatchActor`：负责 watch 全量与增量事件，发布节点变化事件。
4. `ProjectionActor`：消费 lease/registration/watch 事件，统一合并并原子发布快照。

### 0.3 当前协作链路

1. 启动链路：`Connect -> WatchActor.Start + LeaseActor.Start + ProjectionActor.Start`。
2. 注册链路：`LeaseGranted -> RegistrationActor 批量写入 -> RegistrationIndexChanged`。
3. 读模型链路：`ProjectionActor 收事件 -> 合并 Discovery + Topology -> 发布 ExportSnapshot`。
4. 注册通知链路：`RegistrationActor 完成写入 -> 发布 EventRegistrationChanged -> 外部订阅者`（不进入 ExportSnapshot）。
5. 重建链路：`LeaseExpired -> service 标记 stale -> 新 LeaseGranted -> 重放写入 -> 发布新快照版本`。

### 0.4 单一快照出口

1. 对外只读 `ExportSnapshot`。
2. `ByPath/ByName/ByID` 与 `NodesByPath` 必须在同一版本快照中一起发布。
3. 快照发布者只有 `ProjectionActor`。

---

## 一、现状模型综述

### 1.1 架构图（当前）

```
调用方 (module/EtcdModule)
  │
  ├──── Start/Stop/Tick ──────────────────────────────────────┐
  │                                                           │
  ▼                                                           ▼
EtcdCluster ──── lifecycleState (RWMutex) ────────── ClusterState 状态机
  │
  ├── keepaliveState
  │     ├── RegistrationManager  (RWMutex) ──── map[path]*EtcdRegistration
  │     │     └── SetLease() → TriggerMaybeUpdate() → etcd.Put()   ← I/O 阻塞!
  │     └── leaseID / kaCtx (context.Background()) ← 未绑定 runCtx!
  │
  ├── watcherState
  │     └── EtcdWatcherManager  (内部 RWMutex) ──── map[prefix]*EtcdWatcher
  │           └── handleWatcherEvent() ──► dispatchOnEventLoop()
  │
  ├── discoveryState
  │     └── EtcdDiscoverySet  (RWMutex) ──── 3 索引 (path/id/name)
  │           └── 事件发布闭包在锁外执行 ✓
  │
  └── eventState
        ├── EventManager  (RWMutex) ──── map[EventType][]callback
        │     └── Publish() 同步调用所有 callback
        └── clusterEventLoop  (channel[1024])
              └── run() goroutine ←── 唯一事件分发线程
```

### 1.2 数据流（当前）

```
etcd keepalive channel
    │
    ▼
runClusterKeepalive() goroutine
    │
    ├── LeaseGranted → publishClusterEvent(async) → 入队 loop.ch
    └── LeaseExpired → publishClusterEvent(async) → 入队 loop.ch

loop.run() goroutine (单线程)
    │
    ▼
em.Publish(event)           ← 同步调用所有 subscriber
    │
    ├── leaseEventBridge callback
    │       └── km.SetLease(leaseID)         ← 在 loop goroutine 上执行!
    │             └── for each EtcdRegistration:
    │                   TriggerMaybeUpdate(context.Background())
    │                         └── refresh() → etcd.Put()    ← 阻塞 I/O !
    │
    └── 其他 subscriber callback
          └── 等待 leaseEventBridge 执行完才能运行
```

### 1.3 模块协作关系（当前）

| 模块 | 职责 | 拥有的状态 | 对外接口 |
|------|------|-----------|---------|
| `EtcdCluster` | 整体编排；生命周期 | lifecycleState, configState | Start/Stop/Tick/Subscribe |
| `clusterEventLoop` | 串行分发事件任务 | 无业务状态 | post(fn) |
| `EventManager` | pub/sub 注册与分发 | 订阅表 (RWMutex) | Subscribe/Publish/Close |
| `RegistrationManager` | 管理服务注册表 | keepalives map (RWMutex) | SetLease/Add/Remove/GetLease |
| `EtcdRegistration` | 单个服务注册 KV | state/info/leaseOwner (RWMutex) | Start/Stop/TriggerMaybeUpdate |
| `EtcdWatcherManager` | watch 流管理 | prefixes/watchers (RWMutex) | Add/Remove/ActiveAll |
| `EtcdDiscoverySet` | 发现节点索引 | 3 map (RWMutex) | HandleWatcherEvent/ApplySnapshot |
| `EtcdWatcher` | 单条 etcd watch 流 | stream/revision | Start/Stop |

### 1.4 已知问题清单

| # | 描述 | 严重度 | 当前状态 |
|---|------|--------|---------|
| P1 | `SetLease()` 在 eventLoop goroutine 上执行同步 etcd I/O | 🔴 严重 | 未修复 |
| P2 | `kaCtx` 来自 `context.Background()` 而非 `runCtx` | 🔴 严重 | 未修复 |
| P3 | `publishClusterEventSync` 在 `ensureClusterLease`(singleflight) 内同步发布 | 🟠 中 | 未修复 |
| P4 | `onDestroy` 在 `removeNodeWithEvent` 关键路径上执行 | 🟡 低 | 未修复 |
| P5 | `GetDiscoveryInfo()` 裸指针暴露 (WithDiscoveryInfo 是替代方案，未删旧接口) | 🟡 低 | 部分 |
| P6 | 多处 `context.Background()` 无超时 (TriggerMaybeUpdate L862) | 🟠 中 | 未修复 |

---

## 二、v2 目标架构：四层模型

### 2.0 核心原则

**单写者原则**：每一类可变状态只有唯一的 goroutine 拥有写权限，其他 goroutine 只能通过消息请求状态变更。

```
Actor 内部状态 ──► 只有 Actor 自己的 mailbox goroutine 可写
外部读取      ──► 通过原子指针或 copy-on-write 快照获取不可变副本
外部写请求    ──► 发送消息到 Actor mailbox，不阻塞调用方
```

### 2.1 分层概述

```
┌──────────────────────────────────────────────────────────────┐
│  Layer 4: 对外接口层  etcd_module_v2/module.go               │
│  EtcdModule                                                  │
│  Init / Start / Stop / Tick / Reload                         │
│  RegisterService / UnregisterService                         │
│  AddTopologyWatcher / AddDiscoveryWatcher / GetSnapshot      │
└───────────────────────────┬──────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│  Layer 3: 业务编排层  etcd_module_v2/internal/orchestrator/  │
│  LeaseActor         RegistrationActor                        │
│  WatchActor         ProjectionActor(统一投影Actor)           │
└───────────────────────────┬──────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│  Layer 2: 并发架构层  etcd_module_v2/internal/runtime/       │
│  actorBase[T]    EventBus    moduleActorRuntime               │
└───────────────────────────┬──────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│  Layer 1: etcd 连接层  etcd_module/client/  (保持不变)       │
│  EtcdClient: Put / Get / Delete / Watch                      │
│              Grant / Revoke / KeepAlive                      │
└──────────────────────────────────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│  Layer 0: SDK 层  go.etcd.io/etcd/client/v3                  │
│  clientv3: grpc transport / Lease / KV / Watch API           │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 目录结构

```
etcd_module_v2/                           ← package modulev2（正式对外包）
├── module.go                             ← EtcdModule facade（对外接口层）
├── types.go                              ← 对外类型（WatcherSender, TopologyInfo 等）
│
└── internal/
    ├── runtime/                          ← 并发架构层（无业务语义）
    │   ├── actor.go                      ← actorBase[T], actorBox[T]
    │   ├── event_bus.go                  ← EventBus（跨 Actor 事件总线）
    │   └── lifecycle.go                  ← moduleActorRuntime（Spawn/Stop/WaitGroup）
    │
    ├── snapshot/                         ← 快照读模型层（只读，不写 etcd）
    │   ├── registration_snapshot.go      ← 注册索引快照（ByPath/ByName/ByID）
    │   └── export_snapshot.go            ← 对外导出快照聚合
    │
    └── orchestrator/                     ← 业务编排层（4 个 Actor）
        ├── lease_actor.go                ← 租约状态机
        ├── registration_actor.go         ← 服务注册编排（bypath/byname/byid/topology）
        ├── watch_actor.go                ← watch 流管理
        └── projection_actor.go           ← 统一快照投影（拓扑 + 注册索引）

etcd_module/client/                       ← etcd 连接层（不变，被 v2 直接依赖）
go.etcd.io/etcd/client/v3                ← etcd 官方 SDK（由 client 层封装）
etcd_module/cluster/                      ← 历史层（v1 module 使用；v2 不再深度依赖）
```

### 2.3 各层边界

| 层 | 包路径 | 可依赖的下层 | 职责 |
|----|--------|-----------|------|
| 对外接口层 | `etcd_module_v2/` | orchestrator | 稳定 API；将外部调用翻译为 Actor 消息 |
| 业务编排层 | `internal/orchestrator/` | runtime, client | 4 个 Actor；业务状态机；EventBus 事件发布 |
| 并发架构层 | `internal/runtime/` | （无业务依赖） | 泛型 mailbox、goroutine 生命周期、事件总线 |
| etcd 连接层 | `etcd_module/client/` | （etcd clientv3） | 纯 etcd 操作；无业务状态 |
| SDK 层 | `go.etcd.io/etcd/client/v3` | grpc/etcd server | 官方 clientv3 API 与连接复用 |

> **跨层原则**：上层可依赖下层，下层绝不 import 上层。业务 Actor 不感知 "module 是什么"，只接受 etcdClient 注入。

### 2.5 统一投影原则（Topology + Index）

Topology 视图与 bypath/byname/byid 索引视图统一纳入同一个投影 Actor（ProjectionActor）。

理由：

1. 快照诉求一致：两者都需要对外提供可原子读取的一致快照。
2. rebuild 触发一致：都受 Lease 变化、配置变更、watch 事件影响。
3. 一致性窗口更小：单 Actor 单写者可在一次消息处理中同时更新 Topology 与 Registration 子视图，避免跨 Actor 读到不一致版本。
4. 接口更稳定：对外只暴露一个 `GetSnapshot()`，调用方不需要拼接多份视图。

因此，RegistrationActor 负责“写模型”（向 etcd 写入），ProjectionActor 负责“读模型”（导出统一快照）。

命名澄清：

1. `ProjectionActor` 是统一快照投影 actor。
2. 它仅处理 Watch 事件（Discovery + Topology），**不**订阅 `EventRegistrationChanged`。
3. `RegistrationActor` 不负责对外快照发布，只负责写入 etcd 与维护写侧状态。
4. `ExportSnapshot` 只反映 Watch 结果（远端节点），自节点注册状态通过 `EventRegistrationChanged` 独立广播，不进入快照。

### 2.4 与 etcd_module/cluster 的关系演进

| 阶段 | v2 对 cluster 的依赖 | cluster 的角色 |
|------|---------------------|---------------|
| **现在**（过渡）| v2 正在从 `cluster.EtcdCluster` 过渡到直连 etcdClient | cluster = 中间层 |
| **中期**（Phase B-C 完成后）| v2 Actors 直接依赖 etcdClient 接口（含 LeaseActor Grant/KeepAlive）；`bindLeaseEventBridge` 对 v2 关闭 | cluster = 遗留层 |
| **长期**（v1 废弃后）| cluster 层降级合并至 client 层，或整体移除 | cluster 废弃 |

---

## 三、业务生命周期与 Actor 协作

### 3.1 启动流程

```
Init(ctx, cfg)
  └── [连接层] etcdClient.Connect()
        └── 连接建立后，并行启动：
              ├── WatchActor.Start()      ← 开始 watch 远端节点变化
              ├── LeaseActor.Start()      ← 开始申请租约（"续期 lease 的 task" 的载体）
          └── ProjectionActor.Start() ← 准备接收远端节点与注册索引投影事件

RegistrationActor 的注册输入来自配置（Init/Reload），不是 watch 自己 key 的反推。
```

### 3.2 Actor 协作时序

```
LeaseActor         EventBus              RegistrationActor       ProjectionActor
     │                  │                        │                     │
     │ Grant(ttl)        │                        │                     │
     │─► etcdClient     │                        │                     │
     │◄─ leaseID        │                        │                     │
     │                   │                        │                     │
     │─ publish ────────► LeaseGranted(leaseID)  │                     │
     │                   │ ──────────────────────►│                     │
     │                   │                        │ PutWithLease(bypath)│
    │                   │                        │ PutWithLease(byname)│
     │                   │                        │ PutWithLease(byid)  │
     │                   │                        │ PutWithLease(topo)  │
     │                   │                        │ （批量，带 timeout ctx）
    │                   │ ◄──────────────────────│                     │
    │                   │ EventRegistrationChanged(bypath/byname/byid) │
    │                   │ ──────► 外部订阅者（ProjectionActor 不消费此事件）
     │                   │                        │                     │
     │ [keepalive goroutine 运行中]               │                     │
     │ keepalive expired  │                        │                     │
     │─ publish ────────► LeaseExpired            │                     │
     │                   │ ──────────────────────►│                     │
     │                   │                        │ 清空本地注册状态     │
     │                   │                        │ （etcd 侧 lease TTL │
     │                   │                        │  过期自动 GC）       │
     │ Rebuilding→Acquiring                       │                     │
     │ ... （循环）       │                        │                     │
     │                   │                        │                     │
     │            WatchActor                      │                     │
    │                   │ NodeUp/NodeDown ───────────────────────────►│
    │                   │ SnapshotLoaded ────────────────────────────►│
    │                   │                        │                     │ 统一更新 atomic snapshot
```

### 3.3 LeaseActor 状态机

```
         Start / Tick
              │
              ▼
           ┌─────┐
           │ Idle │◄────────────────────────────────────┐
           └──┬──┘                                      │
              │ Grant(ttl)                               │
              ▼                                          │
         ┌──────────┐                                    │
         │ Acquiring │── Grant() fail ──► Idle (Tick 重试)│
         └────┬─────┘                                    │
              │ Grant() ok                               │
              ▼                                          │
          ┌────────┐                                     │
          │ Active │── keepalive goroutine 启动           │
          └───┬────┘                                     │
              │                                          │
              ├── keepalive renewed ──► Active           │
              │                                          │
              ├── keepalive expired ──► Rebuilding       │
              │        └── backoff ──► Acquiring         │
              │                                          │
              └── Stop / Revoke ──► Revoking ───────────►│

输出事件（通过 EventBus）：
    LeaseGranted(leaseID, ttl)  → RegistrationActor, 外部订阅者
  LeaseExpired(leaseID)       → RegistrationActor, 外部订阅者
  LeaseReleased(leaseID)      → 外部订阅者（Stop 路径）

Rebuild 触发条件：
    1) keepalive channel 关闭或租约续期失败
    2) RegistrationActor 上报 lease 不匹配（写入返回 lease related error）
    3) 配置变更触发租约 TTL 或注册集合变化

统一快照重建规则：
    - LeaseGranted：RegistrationActor 执行批量 replay 写入；所有 replay 完成后发布一次 `EventRegistrationChanged`（供外部订阅者）
    - LeaseExpired：RegistrationActor 标记本地 stale；`ExportSnapshot` 不含 Registration 子视图，快照无需因租约变化而更新
    - Watch SnapshotLoaded：重建 Topology 子视图，ProjectionActor 原子发布新快照
    - 对外读取始终只读最后一次原子发布的完整快照（含 Discovery + Topology，不含 Registration）
```

---

## 四、各 Actor 详细设计

### 4.1 LeaseActor

```go
// 消息类型（sealed interface）
type leaseMsg interface{ leaseMsgKind() }

type LeaseMsgType uint8

const (
    LeaseMsgStart LeaseMsgType = iota + 1
    LeaseMsgStop
    LeaseMsgRenewed
    LeaseMsgExpired
)

func (t LeaseMsgType) Name() string {
    switch t {
    case LeaseMsgStart:
        return "LeaseMsgStart"
    case LeaseMsgStop:
        return "LeaseMsgStop"
    case LeaseMsgRenewed:
        return "LeaseMsgRenewed"
    case LeaseMsgExpired:
        return "LeaseMsgExpired"
    default:
        return "LeaseMsgUnknown"
    }
}

type leaseMsgStart   struct{ ttl int64 }
type leaseMsgStop    struct{ reply chan<- error }
type leaseMsgRenewed struct{ leaseID clientv3.LeaseID; ttl int64 }
type leaseMsgExpired struct{ leaseID clientv3.LeaseID }

// Actor（架构层 actorBase + 业务状态，仅 Run goroutine 读写，无需加锁）
type LeaseActor struct {
    actorBase[leaseMsg]                  // mailbox + run loop
    etcdClient etcdClientIface           // 注入：etcd 连接层
    eventBus   EventBus                  // 注入：事件总线
    // 内部状态
    state      leaseState                // Idle / Acquiring / Active / Rebuilding
    currentID  clientv3.LeaseID
    kaCancel   context.CancelFunc        // 控制 keepalive goroutine 的 cancel
}
// mailbox 容量：4
// 背压策略：post 带 timeout；超时记录 warn，由 Tick 路径重试
```

**KeepAlive goroutine**：由 LeaseActor 通过 `runtime.Spawn()` 启动，绑定 LeaseActor 的子 ctx。
keepalive channel 关闭时向 mailbox 投递 `leaseMsgExpired`（非阻塞，level-triggered）。

> **注意**："续期 lease 的 task" = LeaseActor 本身是其载体；keepalive goroutine 是其内部实现。不需要单独的 Task 抽象类型。

### 4.2 RegistrationActor

```go
// 消息类型（sealed interface）
type regMsg interface{ regMsgKind() }

type RegMsgType uint8

const (
    RegMsgLeaseGranted RegMsgType = iota + 1
    RegMsgLeaseExpired
    RegMsgAddService
    RegMsgRemoveService
    RegMsgSyncTopo
    RegMsgFlushTopo
    RegMsgReplayService
)

func (t RegMsgType) Name() string {
    switch t {
    case RegMsgLeaseGranted:
        return "RegMsgLeaseGranted"
    case RegMsgLeaseExpired:
        return "RegMsgLeaseExpired"
    case RegMsgAddService:
        return "RegMsgAddService"
    case RegMsgRemoveService:
        return "RegMsgRemoveService"
    case RegMsgSyncTopo:
        return "RegMsgSyncTopo"
    case RegMsgFlushTopo:
        return "RegMsgFlushTopo"
    case RegMsgReplayService:
        return "RegMsgReplayService"
    default:
        return "RegMsgUnknown"
    }
}

type regMsgLeaseGranted  struct{ leaseID clientv3.LeaseID }
type regMsgLeaseExpired  struct{}
type regMsgAddService    struct{ info *pb.AtappDiscovery; path string; ttl int64; reply chan<- error }
type regMsgRemoveService struct{ path string; reply chan<- error }
type regMsgSyncTopo      struct{}                               // 沿用现有 SyncKeepalive
type regMsgFlushTopo     struct{ ctx context.Context; reply chan<- error }

// 内部状态（仅 Run goroutine 读写）
type registrationActorState struct {
    leaseID    clientv3.LeaseID         // 当前有效 lease，0 = 无租约
    services   map[string]serviceEntry  // 所有待注册服务（key = serviceKey）
    // ... topology keepalive 字段（沿用现有实现）
}

type serviceEntry struct {
    info        *pb.AtappDiscovery
    ttl         int64
    desired     bool
    registered  bool
    stale       bool
    retryCount  int
    lastError   error
    updatedAt   time.Time
}
```

**注册类型与路径**（统一由 RegistrationActor 维护并上报变更）：

| 类型 | path 来源 | value 类型 |
|------|----------|-----------|
| by-path | 外部指定 | `AtappDiscovery` |
| by-name | 自动计算（name 前缀 + name） | `AtappDiscovery` |
| by-id | 自动计算（id 前缀 + id） | `AtappDiscovery` |
| topology keepalive | topology 前缀 + name-id | `AtappTopologyInfo` |

所有注册使用**同一个 lease**（来自 `LeaseGranted` 事件）。

`bypath/byname/byid` 的 payload 使用同一份 `AtappDiscovery` 语义快照（可按写入路径独立序列化），
并共享同一个 lease 生命周期。

**LeaseExpired 行为**：仅清空 `registered = false`，不主动 Delete —— lease 过期后 etcd 侧 TTL 自动 GC 全部 key。

### 4.3 WatchActor

```go
// 消息类型
type watchMsg interface{ watchMsgKind() }

type WatchMsgType uint8

const (
    WatchMsgAddPrefix WatchMsgType = iota + 1
    WatchMsgRemovePrefix
    WatchMsgActiveAll
)

func (t WatchMsgType) Name() string {
    switch t {
    case WatchMsgAddPrefix:
        return "WatchMsgAddPrefix"
    case WatchMsgRemovePrefix:
        return "WatchMsgRemovePrefix"
    case WatchMsgActiveAll:
        return "WatchMsgActiveAll"
    default:
        return "WatchMsgUnknown"
    }
}

type watchMsgAddPrefix    struct{ prefix string }
type watchMsgRemovePrefix struct{ prefix string }
type watchMsgActiveAll    struct{}

// 内部状态（仅 Run goroutine 读写）
type watchActorState struct {
    prefixes map[string]struct{}
    streams  map[string]watchStreamHandle
}
```

**输出**（通过 EventBus）：

| 事件 | 触发时机 |
|------|---------|
| `SnapshotLoading` | 全量 watch 开始前（通知 ProjectionActor 清空） |
| `SnapshotLoaded(events)` | 初始快照拉取完成 |
| `NodeUp/Down/Update(event)` | 增量 watch 事件 |

### 4.4 ProjectionActor（统一投影 Actor）

现有 `etcd_module_v2/projection_actor.go` 实现可作为基础：
- `atomic.Pointer[projectionActorSnapshot]` 设计保留（读零锁）
- mailbox 容量 256；dedupe 机制保留
- **迁移工作**：移至 `internal/orchestrator/projection_actor.go`；从直接函数回调改为订阅 EventBus

ProjectionActor 对外导出的快照不只包含远端拓扑，还包含本节点注册索引视图。
两者是同一份原子快照中的两个子区段：

```go
type ProjectionMsgType uint8

const (
    ProjectionMsgApplyEvent ProjectionMsgType = iota + 1
    ProjectionMsgRebuildFromSnapshot
    ProjectionMsgReset
)

func (t ProjectionMsgType) Name() string {
    switch t {
    case ProjectionMsgApplyEvent:
        return "ProjectionMsgApplyEvent"
    case ProjectionMsgRebuildFromSnapshot:
        return "ProjectionMsgRebuildFromSnapshot"
    case ProjectionMsgReset:
        return "ProjectionMsgReset"
    default:
        return "ProjectionMsgUnknown"
    }
}
```

```go
type ExportSnapshot struct {
    // Version is monotonically incremented on every atomic publish.
    Version     uint64
    PublishedAt time.Time
    // Cause identifies which sub-tree triggered this publish.
    Cause     SnapshotCause
    Discovery DiscoverySetSnapshot
    Topology  TopologySnapshot
    // Note: Registration is intentionally absent.
    // ExportSnapshot only reflects Watch results (remote nodes seen via etcd).
    // Self-node registration state is published via EventRegistrationChanged.
}
```

说明：RegistrationActor 负责写入 etcd（写模型）；ProjectionActor 负责 Watch 结果的统一投影与导出（读模型）。
`Registration` 子视图**不**进入 `ExportSnapshot`；自节点注册状态通过 `EventRegistrationChanged` 事件独立广播给外部订阅者。

### 4.4.1 Registration 快照子结构（统一模型内）

`ByPath/ByName/ByID` **就是同一个统一快照模型的一部分**，不是第二套快照系统。

`internal/snapshot/registration_snapshot.go` 的定位是“子结构定义与拷贝工具”，
它不会单独发布快照，也不会维护独立版本号。

职责：

1. 定义 Registration 子视图的数据结构（ByPath/ByName/ByID）。
2. 提供 `Clone()` 与 `DeepCopy()` 工具，供统一快照组装时调用。
3. 由 ProjectionActor 将其与 Topology 子视图合并后一次 `atomic.Store` 发布。

示意：

```go
type RegistrationSnapshot struct {
    LeaseID   clientv3.LeaseID
    ByPath    map[string]*pb.AtappDiscovery
    ByName    map[string]*pb.AtappDiscovery
    ByID      map[uint64]*pb.AtappDiscovery
    UpdatedAt time.Time
}

func (s *RegistrationSnapshot) Clone() *RegistrationSnapshot
func (s *RegistrationSnapshot) DeepCopy() *RegistrationSnapshot
```

说明：

- “模块”是指统一快照内的子结构定义，不是新 actor。
- 写操作仍只在 RegistrationActor 执行；快照发布只有一个出口：ProjectionActor。
- 对外读取只读 `ExportSnapshot`，不直接读取 `RegistrationSnapshot`。

### 4.5 架构层 runtime

```
internal/runtime/
├── actor.go       ← actorBase[T], actorBox[T]（从现有 actor_runtime.go 拆出）
├── event_bus.go   ← EventBus 接口（适配 etcd_module/events.EventManager）
└── lifecycle.go   ← moduleActorRuntime（Spawn/Stop/WaitGroup）
```

**EventBus 事件表**：

| 事件类型 | 生产者 | 消费者 |
|---------|--------|--------|
| `LeaseGranted` | LeaseActor | RegistrationActor, 外部订阅者 |
| `LeaseExpired` | LeaseActor | RegistrationActor, 外部订阅者 |
| `LeaseReleased` | LeaseActor | 外部订阅者 |
| `RegistrationIndexChanged` | RegistrationActor | ProjectionActor, 外部订阅者 |
| `NodeUp/Down/Update` | WatchActor | ProjectionActor, 外部订阅者 |
| `SnapshotLoading` | WatchActor | ProjectionActor |
| `SnapshotLoaded` | WatchActor | ProjectionActor, 外部订阅者 |

EventBus 直接适配现有 `etcd_module/events.EventManager`，不重复实现。

### 4.6 Actor 与 Event 消息格式规范

本节定义两类消息：

1. Actor mailbox 内部命令消息（Command Message）
2. EventBus 跨 Actor 领域事件（Domain Event）

> 结论：保留两类消息是必要的；它们的“方向、语义、背压策略”不同。

#### 4.6.0 当前字段结构（实现基线）

本小节定义“当前结构”在消息字段层面的含义：

1. `Message` 是 actor 内部命令，强调最小字段与应答控制。
2. `Event` 是跨 actor 结果广播，强调顺序、代次、幂等与可观测性。

统一命名枚举（减少 string 常量散落）：

```go
type ActorName uint8

const (
    ActorLease ActorName = iota + 1
    ActorRegistration
    ActorWatch
    ActorProjection
)

func (n ActorName) Name() string {
    switch n {
    case ActorLease:
        return "lease"
    case ActorRegistration:
        return "registration"
    case ActorWatch:
        return "watch"
    case ActorProjection:
        return "projection"
    default:
        return "unknown"
    }
}
```

Message（Actor mailbox）字段分级：

| 字段 | 级别 | 作用 | 备注 |
|------|------|------|------|
| `Meta.RequestID` | 可选 | 请求关联与问题排查 | 不参与业务分支 |
| `Meta.Source` | 可选 | 标识消息来源（api/watch/lease） | 用于日志与指标 |
| `Meta.CreatedAt` | 可选 | 统计排队时延、超时诊断 | 不用于状态判断 |
| `Reply chan<- T` | 条件必需 | 需要同步返回结果时应答 | fire-and-forget 消息不带该字段 |
| `LeaseID` | 条件必需 | 绑定写入到当前租约 | 仅 lease 相关命令需要 |
| `Info/Path/TTL` 等业务字段 | 必需 | 承载该命令的最小业务参数 | 严禁使用大而全通用 payload |

Message 明确不需要的字段：

1. 不需要 `Sequence`（mailbox 已天然 FIFO）。
2. 不需要 `DedupeKey`（命令默认不可去重，重复由业务状态机处理）。
3. 不需要统一 `Payload any`（保持静态字段可读性与类型安全）。

Event（EventBus Envelope）字段分级：

| 字段 | 级别 | 作用 | 备注 |
|------|------|------|------|
| `Type` | 必需 | 标识事件语义 | 消费路由主键 |
| `Version` | 必需 | 事件 schema 版本 | 兼容演进 |
| `Source` | 必需 | 标识生产者 actor | 配合 Sequence 使用 |
| `Sequence` | 必需 | Source 内单调序 | 保证同源有序处理 |
| `LeaseEpoch` | 条件必需 | 标识租约代次 | lease/rebuild 相关事件必须带 |
| `OccurredAt` | 必需 | 观测时间点 | 指标与审计 |
| `TraceID` | 可选 | 跨消息链路追踪 | 可由入口注入 |
| `DedupeKey` | 条件必需 | 幂等去重键 | watch/registration 高频事件建议必带 |
| `Payload` | 必需 | 事件数据体 | 类型由 `Type` 决定 |

Event 明确不需要的字段：

1. 不需要 `Reply`（事件不承担命令应答语义）。
2. 不需要可写共享引用（payload 应视为发布后只读）。
3. 不需要跨 Source 的全局总序（仅要求 Source 内顺序 + LeaseEpoch 代次隔离）。

最小落地规则：

1. 进程内动作控制：优先 Message。
2. 跨 actor 状态传播：优先 Event。
3. Message 最小字段化，Event 可观测化；两者不互相替代。

#### 4.6.1 Actor mailbox 消息格式（内部）

Actor 内部采用 sealed interface + 每个消息结构体独立字段，不做统一大而全 payload。

```go
// 所有 actor 命令消息可选共享的最小元信息
type MsgMeta struct {
    // 仅用于日志与排障，不用于业务判断
    RequestID string
    Source    ActorName // module/api/watch/lease 等来源枚举化
    CreatedAt time.Time
}

// 示例：RegistrationActor 消息
type regMsg interface{ regMsgKind() }

type regMsgAddService struct {
    Meta  MsgMeta
    Info  *pb.AtappDiscovery
    Path  string
    TTL   int64
    Reply chan<- error
}

type regMsgLeaseGranted struct {
    Meta    MsgMeta
    LeaseID clientv3.LeaseID
}

type regMsgLeaseExpired struct {
    Meta MsgMeta
}
```

约束：

1. mailbox 消息只在进程内传递，不做网络序列化。
2. 每条消息只携带该命令必需字段，避免通用 Any 结构。
3. 需要响应时使用 `Reply chan<- T`，无响应命令不带 reply。
4. 所有业务状态变更只在 actor 单 goroutine 执行。

#### 4.6.2 EventBus 事件格式（跨 Actor）

EventBus 事件使用统一 Envelope，便于去重、排序、重放与快照一致性。

```go
type EventType string

type EventSource uint8

const (
    EventSourceLeaseActor EventSource = iota + 1
    EventSourceRegistrationActor
    EventSourceWatchActor
    EventSourceProjectionActor
)

func (s EventSource) Name() string {
    switch s {
    case EventSourceLeaseActor:
        return "lease-actor"
    case EventSourceRegistrationActor:
        return "registration-actor"
    case EventSourceWatchActor:
        return "watch-actor"
    case EventSourceProjectionActor:
        return "projection-actor"
    default:
        return "unknown-actor"
    }
}

const (
    EventLeaseGranted           EventType = "lease.granted"
    EventLeaseExpired           EventType = "lease.expired"
    EventLeaseReleased          EventType = "lease.released"
    EventRegistrationChanged    EventType = "registration.index.changed"
    EventWatchSnapshotLoading   EventType = "watch.snapshot.loading"
    EventWatchSnapshotLoaded    EventType = "watch.snapshot.loaded"
    EventWatchNodeUp            EventType = "watch.node.up"
    EventWatchNodeDown          EventType = "watch.node.down"
    EventWatchNodeUpdate        EventType = "watch.node.update"

    // Topology watch events (producer: WatchActor)
    EventWatchTopologyUp              EventType = "watch.topology.up"
    EventWatchTopologyDown            EventType = "watch.topology.down"
    EventWatchTopologyUpdate          EventType = "watch.topology.update"
    EventWatchTopologySnapshotLoading EventType = "watch.topology.snapshot.loading"
    EventWatchTopologySnapshotLoaded  EventType = "watch.topology.snapshot.loaded"
)

type EventEnvelope struct {
    Type       EventType
    Version    uint16        // 事件 schema 版本，初始为 1
    Source     EventSource   // 枚举化来源，日志展示可用 Source.Name()
    Sequence   uint64        // bus 全局单调递增序号，由 EventBus.Publish 统一赋值
    LeaseEpoch uint64        // 租约代次，跨 lease rebuild 的关键字段
    OccurredAt time.Time
    TraceID    string
    DedupeKey  string        // 幂等键，ProjectionActor 可用于去重
    Payload    any
}
```

Payload 建议结构：

```go
type LeaseGrantedPayload struct {
    LeaseID clientv3.LeaseID
    TTL     int64
}

type RegistrationChangedPayload struct {
    LeaseID clientv3.LeaseID
    ByPath  map[string]*pb.AtappDiscovery
    ByName  map[string]*pb.AtappDiscovery
    ByID    map[uint64]*pb.AtappDiscovery
}

// WatchNodePayload is shared by EventWatchNodeUp, Down, and Update.
// Note: EtcdResponseHeader is intentionally omitted.  Revision is globally
// monotonic under Raft consensus (single and multi-member clusters alike),
// making it sufficient for ordering and dedup.  ClusterID validation (防错连)
// belongs at the EtcdClient connection layer (Status() call at init), not
// in every per-event payload.
type WatchNodePayload struct {
    Revision       int64
    Key            string
    Value          *pb.AtappDiscovery // nil for Down events
    RawValue       []byte
    ModRevision    int64
    Version        int64
    CreateRevision int64
}

// WatchTopologyPayload is shared by EventWatchTopologyUp, Down, and Update.
type WatchTopologyPayload struct {
    Revision       int64
    Key            string
    Value          *pb.AtappTopologyInfo // nil for Down events
    ModRevision    int64
    Version        int64
    CreateRevision int64
}
```

#### 4.6.3 Sequence 与 Dedupe 规则

1. `Sequence` 由 `EventBus.Publish` 统一赋值（bus 实例内全局单调递增），不由各 Actor 自行维护。
   - 这提供跨 Source 的全序，比 per-Source 偏序更强；
   - 实现只需在 bus 内 `atomic.Uint64.Add(1)`，Actor 代码零侵入；
   - 零值（0）表示 envelope 未经过 bus 发布，可用于测试断言。
2. `DedupeKey` 规则：
   - lease 事件：`lease:{LeaseEpoch}:{Type}`
   - 注册索引变更：`reg:{LeaseEpoch}:{hash(ByPath,ByName,ByID)}`
   - watch 节点事件：`watch:{revision}:{key}:{type}`
3. ProjectionActor 对同一 `DedupeKey` 重复事件可直接丢弃。

#### 4.6.4 为什么区分两种格式

1. Actor 命令消息追求最小开销与清晰字段。
2. EventBus 事件追求可观测性、幂等重放、快照一致性。
3. 两层职责不同，强行统一会让内部命令变重、事件语义变弱。

补充决策矩阵：

| 场景 | 用 Message（命令） | 用 Event（事件） |
|------|-------------------|------------------|
| API 调用 add/remove service | 是 | 否 |
| LeaseActor 通知租约已建立 | 否 | 是 |
| RegistrationActor 申请写 etcd | 是 | 否 |
| Registration 索引变更通知快照层 | 否 | 是 |
| Watch 增量节点变化广播 | 否 | 是 |

如果只保留一种消息：

- 全用 Event：会丢失“命令应答 + 超时控制”能力，API 行为不稳定。
- 全用 Message：会把广播语义硬塞成点对点调用，耦合升高且不利于订阅扩展。

因此建议继续保留 Message + Event 两类消息，但共享最小公共元信息字段（如 TraceID/OccurredAt）。

#### 4.6.5 Rebuild 流程中的双协议协同

本节回答：Command Message 与 Domain Event 在 rebuild 中如何配合完成快照更新。

核心分工：

1. Message 负责驱动“动作”（写入、状态变更、重试）。
2. Event 负责广播“结果”（lease 变化、索引变化、watch 变化）。
3. ProjectionActor 只消费 Event，统一发布原子快照。

标准流程（Lease 失效后重建）：

```text
阶段 A：触发
    1) LeaseActor 检测 keepalive 断开（内部 message：leaseMsgExpired）
    2) LeaseActor 更新本地状态为 Rebuilding
    3) LeaseActor 发布 EventLeaseExpired(LeaseEpoch=N)

阶段 B：准备重放
    4) RegistrationActor 收到 EventLeaseExpired
    5) RegistrationActor 将 services[*] 标记 stale=true, registered=false
    6) RegistrationActor 不执行 delete（依赖 etcd lease TTL 回收）

阶段 C：新 lease 建立
    7) LeaseActor 通过 message 流程重试 Grant 成功（leaseID=new, LeaseEpoch=N+1）
    8) LeaseActor 发布 EventLeaseGranted(LeaseEpoch=N+1)

阶段 D：写模型重放
    9) RegistrationActor 收到 EventLeaseGranted 后，对 stale service 逐个执行 message 命令：
         bypath -> byname -> byid -> topology
 10) 每个 service 成功后更新本地 serviceEntry，并汇总当前索引视图
 11) RegistrationActor 发布 EventRegistrationChanged(LeaseEpoch=N+1, payload=索引快照)

阶段 E：读模型更新
 12) ProjectionActor 收到 EventRegistrationChanged（以及并行的 watch 事件）
 13) ProjectionActor 校验 LeaseEpoch 与 DedupeKey，丢弃旧代次/重复事件
 14) ProjectionActor 合并 Topology 子视图 + Registration 子视图
 15) ProjectionActor 原子发布新 ExportSnapshot(version++)
```

失败分支：

1. RegistrationActor 某一步写入失败：
     - 保留 `stale=true`，`retryCount++`，下一轮 message 重试。
     - 不发布成功版 RegistrationChanged（避免快照误报）。
2. ProjectionActor 收到旧 LeaseEpoch 事件：
     - 直接丢弃，不影响当前快照。
3. Watch 事件先于 RegistrationChanged 到达：
     - 可先更新 Topology 子视图并发布；待 RegistrationChanged 到达再发布新版本。
     - 通过 version 单调递增保证读取方可判断新旧。

最小实现规则：

1. 所有“写 etcd”动作必须由 Message 驱动，禁止直接在 Event 回调里做 I/O。
2. 所有“对外可见状态变化”必须通过 Event 传播给 ProjectionActor。
3. ProjectionActor 是唯一快照发布者，外部读取只认 ExportSnapshot。
4. `ByPath/ByName/ByID` 与 `NodesByPath` 必须在同一版本 `ExportSnapshot` 中一起发布。

示例（时间线）：

```text
t0  lease=100, epoch=7, snapshot.version=42
t1  keepalive 断开 -> EventLeaseExpired(epoch=7)
t2  RegistrationActor 标记 serviceA stale
t3  LeaseActor grant 成功 -> lease=101, epoch=8 -> EventLeaseGranted(epoch=8)
t4  RegistrationActor 重放写入 serviceA 四条路径成功
t5  EventRegistrationChanged(epoch=8, serviceA indexes)
t6  ProjectionActor 合并并发布 snapshot.version=43
```

结果：

- rebuild 期间没有跨 actor 锁竞争。
- 快照更新由统一发布点完成，可追踪、可重放、可去重。

#### 4.6.6 Rebuild 关键实现（基于 Message/Event 两种结构）

本小节给出可直接落地的实现骨架：

1. LeaseActor 通过 Message 完成租约状态迁移。
2. LeaseActor/RegistrationActor 通过 Event 广播阶段结果。
3. ProjectionActor 仅消费 Event，并按 `LeaseEpoch` 与 `DedupeKey` 做快照发布。

实现骨架（一）：LeaseActor 的 lease 变更流程（Message 驱动）

```go
// LeaseActor 单写者状态
type leaseState struct {
    phase      string // idle/acquiring/active/rebuilding
    leaseID    clientv3.LeaseID
    leaseEpoch uint64
}

func (a *LeaseActor) onStart(msg leaseMsgStart) {
    if a.st.phase != "idle" {
        return
    }
    a.st.phase = "acquiring"
    a.tryGrant(msg.ttl)
}

func (a *LeaseActor) tryGrant(ttl int64) {
    id, err := a.etcdClient.Grant(a.actorCtx, ttl)
    if err != nil {
        // 失败保持 acquiring，等待 Tick/backoff 继续 message 重试
        return
    }

    a.st.leaseID = id
    a.st.leaseEpoch++
    a.st.phase = "active"

    // 先启动 keepalive，再发布结果事件
    a.startKeepaliveLoop(id)

    a.publish(EventEnvelope{
        Type:       EventLeaseGranted,
        Version:    1,
        Source:     EventSourceLeaseActor,
        Sequence:   a.nextSeq(),
        LeaseEpoch: a.st.leaseEpoch,
        OccurredAt: time.Now(),
        DedupeKey:  fmt.Sprintf("lease:%d:%s", a.st.leaseEpoch, EventLeaseGranted),
        Payload: LeaseGrantedPayload{LeaseID: id, TTL: ttl},
    })
}

func (a *LeaseActor) onKeepaliveExpired(oldID clientv3.LeaseID) {
    if a.st.phase != "active" || a.st.leaseID != oldID {
        return
    }

    a.st.phase = "rebuilding"
    a.publish(EventEnvelope{
        Type:       EventLeaseExpired,
        Version:    1,
        Source:     EventSourceLeaseActor,
        Sequence:   a.nextSeq(),
        LeaseEpoch: a.st.leaseEpoch,
        OccurredAt: time.Now(),
        DedupeKey:  fmt.Sprintf("lease:%d:%s", a.st.leaseEpoch, EventLeaseExpired),
        Payload:    struct{ LeaseID clientv3.LeaseID }{LeaseID: oldID},
    })

    // rebuild 核心：回到 acquiring，进入下一轮 grant
    a.st.phase = "acquiring"
}
```

实现骨架（二）：RegistrationActor 的重放流程（Event 触发 + Message 执行 I/O）

```go
func (a *RegistrationActor) onEventLeaseExpired(ev EventEnvelope) {
    if ev.Type != EventLeaseExpired {
        return
    }

    // 只改本地写模型状态，不做 delete
    for key, svc := range a.services {
        svc.registered = false
        svc.stale = true
        a.services[key] = svc
    }
}

func (a *RegistrationActor) onEventLeaseGranted(ev EventEnvelope) {
    if ev.Type != EventLeaseGranted {
        return
    }

    pl := ev.Payload.(LeaseGrantedPayload)
    a.leaseID = pl.LeaseID
    a.leaseEpoch = ev.LeaseEpoch

    // 将“重放动作”拆成内部 message，避免在 event 回调中直接执行长耗时 I/O
    for key := range a.services {
        a.post(regMsgReplayService{ServiceKey: key})
    }
}

func (a *RegistrationActor) onReplayService(msg regMsgReplayService) {
    svc := a.services[msg.ServiceKey]
    if !svc.desired {
        return
    }

    // 固定写入顺序：bypath -> byname -> byid -> topology
    if err := a.putAllWithLease(svc, a.leaseID); err != nil {
        svc.retryCount++
        svc.lastError = err
        svc.stale = true
        a.services[msg.ServiceKey] = svc
        return
    }

    svc.registered = true
    svc.stale = false
    svc.lastError = nil
    a.services[msg.ServiceKey] = svc

    // 发布 RegistrationChanged 给 ProjectionActor
    a.publish(EventEnvelope{
        Type:       EventRegistrationChanged,
        Version:    1,
        Source:     EventSourceRegistrationActor,
        Sequence:   a.nextSeq(),
        LeaseEpoch: a.leaseEpoch,
        OccurredAt: time.Now(),
        DedupeKey:  a.buildRegistrationDedupeKey(),
        Payload:    a.snapshotRegistrationPayload(),
    })
}
```

实现骨架（三）：ProjectionActor 的快照一致性规则（仅消费 Event）

```go
func (a *ProjectionActor) onEvent(ev EventEnvelope) {
    // 1) 旧代次事件直接丢弃
    if ev.LeaseEpoch < a.curLeaseEpoch {
        return
    }

    // 2) 重复事件去重
    if a.isDuplicated(ev.DedupeKey) {
        return
    }

    // 3) 应用事件到工作副本（单线程）
    work := a.cloneWorkingSnapshot()
    switch ev.Type {
    case EventWatchSnapshotLoaded, EventWatchNodeUp, EventWatchNodeDown, EventWatchNodeUpdate:
        applyWatch(work, ev.Payload)
    case EventRegistrationChanged:
        applyRegistration(work, ev.Payload)
        a.curLeaseEpoch = ev.LeaseEpoch
    case EventLeaseExpired:
        markRegistrationStale(work)
    }

    // 4) 一次原子发布完整快照
    work.Version++
    work.UpdatedAt = time.Now()
    a.snapshot.Store(work.freeze())
}
```

lease 变更主流程（最终定义）：

1. `LeaseActor` 收到失效信号（Message），发布 `EventLeaseExpired(epoch=N)`。
2. `RegistrationActor` 仅标记 stale，不做 delete。
3. `LeaseActor` grant 新 lease 成功（Message），发布 `EventLeaseGranted(epoch=N+1)`。
4. `RegistrationActor` 将重放动作拆成 replay Message 顺序执行 I/O。
5. 每次重放成功后发布 `EventRegistrationChanged(epoch=N+1)`。
6. `ProjectionActor` 按 epoch+dedupe 合并并原子发布新快照。

这套实现确保：

1. 写入动作始终在 actor message 处理路径内。
2. 可见状态始终通过 event 广播。
3. rebuild 期间不会出现跨 actor 锁等待链。

---

## 五、Task 概念决策

**结论：不引入显式 Task 类型。**

| 用途 | 实现方式 |
|------|---------|
| "续期 lease 的 task" | LeaseActor 本身（长期运行的状态机） |
| 往 etcd 提交自身注册（addNode） | RegistrationActor 内部 `serviceEntry` 状态驱动（依赖 lease） |
| `etcdClient.Grant()` | 单次 RPC call，通过 `reply chan<- error` 返回结果（actorBase 现有模式） |
| KeepAlive loop | LeaseActor 通过 `runtime.Spawn()` 管理的内部 goroutine |

RegistrationActor 的最小状态流：

```
NoLease(desired=true, registered=false)
     --(LeaseGranted)--> Applying
Applying
     --(put path/name/id/topology 全成功)--> Registered(registered=true, stale=false)
     --(任一写入失败)--> Retry(stale=true, retryCount++)
Registered
     --(LeaseExpired or config changed)--> Stale(registered=false, stale=true)
Stale
     --(next LeaseGranted)--> Applying
```

addNode 示例（无 Task 类型）：

```text
1) AddNode(serviceA) 到达 RegistrationActor
    - 写 services[serviceA]: desired=true, registered=false, stale=true
2) 当前无 lease
    - 不写 etcd，等待 LeaseGranted
3) LeaseGranted(leaseID=100)
    - 对 services[serviceA] 依序写: bypath -> byname -> byid -> topology（均附 lease=100）
4) 全部成功
    - services[serviceA] 更新为 registered=true, stale=false
5) LeaseExpired
    - services[serviceA] 置 registered=false, stale=true（不主动 delete，依赖 lease TTL 回收）
6) 新 LeaseGranted(leaseID=101)
    - 再次按顺序重放写入，完成 rebuild
```

关键约束：
1. 全流程只由 RegistrationActor 单 goroutine 串行驱动。
2. 旧 lease 的 registered 记录在 lease 变化后必须转 stale 并重放。
3. 单个 service 的提交顺序固定为：`bypath -> byname -> byid -> topology`。

因此，addNode 语义由 `serviceEntry` 状态承载，不需要额外 Task 类型。

---

## 六、实现阶段（v2 重写）

> **每阶段验收标准**：`go test ./... -count=1` 全绿 + `-race` 无竞态 + goroutine 无泄漏

### Phase A：架构层目录就位

- 建立 `internal/runtime/` 目录骨架
- 将现有 `actor_runtime.go` 拆分为 `runtime/actor.go` + `runtime/lifecycle.go`
- 新增 `runtime/event_bus.go`（适配现有 `etcd_module/events`）
- 现有 orchestrator 文件暂维持原位，不移动

### Phase B：LeaseActor

- 新增 `internal/orchestrator/lease_actor.go`
- 实现完整状态机（Idle → Acquiring → Active → Rebuilding）
- LeaseActor 直接持有 etcdClient 接口并调用 `Grant/KeepAlive/Revoke`
- KeepAlive goroutine 绑定 Actor 子 ctx；expired 时非阻塞投 `leaseMsgExpired`
- 通过 EventBus 发布三种 Lease 事件
- 测试：状态转换、mailbox 满时非阻塞、事件正确发布、goroutine 不泄漏

### Phase C：RegistrationActor 扩展

- 在现有 `registration_actor.go` 基础上扩展：增加 bypath / byname / byid 注册能力
- 订阅 `LeaseGranted` → 批量注册；订阅 `LeaseExpired` → 清空 registered 标记
- 注册集合由配置驱动（Init/Reload），并在变更时触发 rebuild
- 使用 `serviceEntry` 状态字段表达 addNode 依赖 lease 完成后再提交（不新增 Task 类型）
- 上报 `RegistrationIndexChanged` 事件供 ProjectionActor 更新导出快照
- 迁移至 `internal/orchestrator/registration_actor.go`
- 测试：lease 变更后重放、AddService 在无 lease 时保持 pending 状态、并发 Add/Remove

### Phase D：WatchActor

- 新增 `internal/orchestrator/watch_actor.go`
- 复用 `etcd_module/watcher/` 底层 stream 能力
- 通过 EventBus 发布 NodeUp/Down/SnapshotLoaded 事件
- 测试：prefix add/remove、事件非阻塞投递到 ProjectionActor

### Phase E：ProjectionActor 迁移 + event 驱动切换

- 移入 `internal/orchestrator/projection_actor.go`
- 改为订阅 WatchActor 发布的 EventBus 事件（替代直接函数回调）
- 同时订阅 `RegistrationIndexChanged` 事件，合并拓扑与索引子视图后一次原子发布
- 验证 `atomic.Pointer` 并发读零竞态（race detector）

### Phase F：EtcdModule facade 完善

- 清理 `module.go` 对 `etcd_module/cluster` 的深度依赖
- 对外方法通过注入的 Actors 实现
- 集成测试：完整 start → register → watch → stop 流程

### Phase G：cluster 层 kaCtx 热修复（独立，可随时合并）

- `etcd_module/cluster/cluster.go`：`context.WithCancel(context.Background())` → `context.WithCancel(c.lifecycle.runCtx)`
- 不依赖其他 Phase
- 验证：`go test ./etcd_module/cluster/... -count=1`

---

## 七、已确认约束与风险

### 已确认约束

| # | 决策 |
|---|------|
| D1 | LeaseActor 直接依赖 etcdClient 接口，并负责 `Grant/KeepAlive/Revoke` |
| D2 | 注册来源为配置驱动（Init/Reload），不是 watch 自身 key 的反推 |
| D3 | bypath / byname / byid 全部纳入导出快照，并共享同一个 lease 生命周期 |
| D4 | Lease 变化或配置变化必须触发 Registration rebuild |

### 风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| v2 LeaseActor 与 cluster `bindLeaseEventBridge` 并存导致双重注册 | 中 | 高 | v2 路径明确跳过 `bindLeaseEventBridge`；两者严格互斥 |
| v2 直接用 etcdClient 后，cluster Tick 路径逻辑失效 | 低 | 中 | 过渡期 v2 仍走 cluster Tick；Phase F 时替换 |
| RegistrationActor LeaseExpired 后不主动 Delete 导致 etcd 中短暂残留 | 低 | 低 | 依赖 etcd lease TTL GC，TTL 内残留是预期行为 |
| ProjectionActor 快照替换频率过高造成 GC 压力 | 低 | 低 | 同 revision 消息合并批处理后一次性替换 |

**回退策略**：每个 Phase 是独立可合并的 PR；Phase N 失败只回滚该 Phase，不影响已合并的前置 Phase。
