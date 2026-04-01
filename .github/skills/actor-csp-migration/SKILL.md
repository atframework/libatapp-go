---
name: actor-csp-migration
description: 'etcd_module Actor/CSP 并发模型迁移。用于：继续实现 Phase 1-5 中任一阶段；评审迁移方案；排查 SetLease I/O 阻塞、kaCtx 生命周期、TopologyActor 快照等并发问题；验证 Actor 设计决策（mailbox 容量、背压策略、atomic.Pointer 用法）。关键词：Actor, CSP, mailbox, RegistrationScheduler, LeaseActor, TopologyActor, 单写者, 并发重构, 服务发现, etcd_module。'
argument-hint: '要实现/评审的 Phase 编号，或具体问题描述（如 "Phase 2 RegistrationScheduler 实现"）'
---

# etcd_module Actor/CSP 并发模型迁移

**迁移方案权威文档**: [actor_csp_iteration_plan.md](../../etcd_module/docs/actor_csp_iteration_plan.md)

---

## 核心原则（每次实现前必读）

| 原则 | 规则 |
|------|------|
| **单写者** | 每类可变状态只有 Actor 自己的 mailbox goroutine 可写，外部只读/发消息 |
| **非阻塞发送** | 向 mailbox 发消息必须非阻塞（select default 丢弃或 level-triggered） |
| **I/O 隔离** | etcd.Put / etcd.Grant 等阻塞 I/O 只能在专属 goroutine 内执行，不准在 event loop 里调用 |
| **接口稳定** | 每个 Phase 公共方法签名 diff 为空；`EtcdCluster` / `EtcdModule` 对外接口不变 |
| **可独立回滚** | 每个 Phase 是独立 PR；Phase N 失败只回滚 N，不影响已合并的前置 Phase |

---

## 迁移阶段速查

> **架构方向（v2.0）**：以 `etcd_module_v2` 为正式包，从零建立四层架构，而非在 cluster 层逐一打补丁。
> 完整设计见 [actor_csp_iteration_plan.md](../../etcd_module/docs/actor_csp_iteration_plan.md)。

| Phase | 核心目标 | 关键文件 | 依赖 |
|-------|----------|---------|------|
| A | 架构层目录就位（internal/runtime/） | `actor_runtime.go` 拆分 | 无 |
| B | `LeaseActor` 状态机 | `internal/orchestrator/lease_actor.go` (新增) | Phase A |
| C | `RegistrationActor` 扩展（bypath/byid） | `internal/orchestrator/registration_actor.go` | Phase B |
| D | `WatchActor` | `internal/orchestrator/watch_actor.go` (新增) | Phase A |
| E | `TopologyActor` 迁移 + EventBus 驱动 | `internal/orchestrator/topology_actor.go` | Phase D |
| F | `EtcdModule` facade 完善 | `module.go` | Phase B-E |
| G | cluster `kaCtx` 热修复（独立可选） | `etcd_module/cluster/cluster.go` | 无 |

---

## 实现流程

### Step 1：确认当前 Phase 状态

```
go test ./... -count=1
```

若当前 Phase 测试全绿 → 进入下一 Phase；若有失败 → 先修复再推进。

### Step 2：阅读对应 Phase 的设计细节

打开 [actor_csp_iteration_plan.md](../../etcd_module/docs/actor_csp_iteration_plan.md) 找到对应章节（三～四大节），确认：
- 新增文件名与位置
- 对外接口变化（预期为零）
- Mailbox 容量与背压策略

### Step 3：实现 Actor 骨架

每个 Actor 标准结构：

```go
type XxxActor struct {
    mailbox chan xxxMsg   // 有界 channel，容量见设计
    done    chan struct{}
    // 内部状态仅此 goroutine 读写，无需加锁
}

// Notify / Send — 供外部调用，必须非阻塞
func (a *XxxActor) Notify() {
    select {
    case a.mailbox <- xxxMsg{}:
    default: // level-triggered：已有信号，不重复入队
    }
}

// Run — launch as goroutine, blocks until ctx done
func (a *XxxActor) Run(ctx context.Context) {
    defer close(a.done)
    for {
        select {
        case <-ctx.Done():
            return
        case msg := <-a.mailbox:
            a.handle(msg)
        }
    }
}
```

### Step 4：集成到 cluster 启动序列

在 `cluster.startInternal()` 中：
1. 创建 Actor 实例
2. 注入依赖（`EventManager`, `RegistrationManager` 等）
3. `c.lifecycle.wg.Add(1)` + `go func() { defer wg.Done(); actor.Run(runCtx) }()`

### Step 5：验证

```bash
# 标准验收
go test ./... -count=1

# 带 race detector（关键路径必跑）
go test -race ./etcd_module/... -count=1

# goroutine 泄漏（需 goleak）
go test -run TestXxx ./etcd_module/cluster/... -v
```

---

## 关键设计决策参考

### Mailbox 容量选择

| Actor | 容量 | 理由 |
|-------|------|------|
| `LeaseActor` | 4 | Grant/Revoke 低频，超时即返回错误 |
| `RegistrationScheduler` | 1 (level-triggered) | 信号语义，多余信号可合并 |
| `WatcherCoordinator` | add/remove 频度 + 1 | 中频操作 |
| `TopologyActor` | 256 | 节点事件可能突发 |

### atomic.Pointer vs channel

- **只读快照**（TopologyActor 快照）→ `atomic.Pointer[T]`，读零锁，零 goroutine
- **状态变更请求**（需 Actor 串行处理）→ bounded channel mailbox
- **跨 Actor 事件通知**（fire-and-forget）→ `EventManager.Publish()` 复用现有 pub/sub

### 何时保留 mutex

- 外部调用方并发写同一结构（如 `RegistrationManager.Add/Remove`）→ 保留 RWMutex
- Actor 内部状态仅 Run goroutine 读写 → 不需要锁

---

## 测试模式

### 非阻塞 Notify 验证模板

```go
func TestSchedulerNotifyIsNonBlocking(t *testing.T) {
    // Arrange：构造满 mailbox
    s := NewRegistrationScheduler(...)
    for i := 0; i < cap(s.signal); i++ {
        s.signal <- struct{}{}
    }
    // Act + Assert：Notify 应在纳秒级返回
    done := make(chan struct{})
    go func() { s.Notify(); close(done) }()
    select {
    case <-done:
    case <-time.After(10 * time.Millisecond):
        t.Fatal("Notify blocked")
    }
}
```

### 接口签名不变验证

每个 Phase 完成后运行：
```bash
# 若有 go-apidiff 或类似工具
go vet ./...
# 手动检查：git diff HEAD~1 --stat | grep ".go" 中不含 public method 变化
```

---

## 常见陷阱

- **context.Background() 陷阱**：RegistrationScheduler 的 TriggerMaybeUpdateAll 必须用带 timeout 的子 ctx，不得用 Background()
- **leaseEventBridge 双重调用**：Phase 2 完成后删除 `bindLeaseEventBridge` 中原有的同步 I/O 路径，否则会并发两次 refresh
- **wg.Add 位置**：必须在 goroutine 启动前调用 `wg.Add(1)`，不能在 goroutine 内部
- **done channel 关闭时机**：`close(a.done)` 放在 `defer` 保证 Stop() 可以安全等待

---

## 参考文件

| 文件 | 用途 |
|------|------|
| [actor_csp_iteration_plan.md](../../etcd_module/docs/actor_csp_iteration_plan.md) | **权威迁移方案**，包含完整架构图、Phase 设计、测试清单、风险矩阵 |
| [etcd_module/cluster/cluster.go](../../etcd_module/cluster/cluster.go) | 集群生命周期；Phase 1/2/3 主要改动点 |
| [etcd_module/registration/](../../etcd_module/registration/) | RegistrationManager / EtcdRegistration；Phase 2 新增 scheduler.go |
| [etcd_module/events/events.go](../../etcd_module/events/events.go) | EventManager；Domain Event Bus 保持不变 |
| [etcd_module/discovery/](../../etcd_module/discovery/) | EtcdDiscoverySet；Phase 4 被 TopologyActor 取代 |
