# etcd Lease 工作机制详解

本文档说明 `service_discovery` 项目中 etcd **Lease（租约）** 的使用方式和完整工作流程。

---

## 核心概念

etcd **Lease（租约）** 是 etcd 的 TTL 机制。绑定了 lease 的 key，在 lease 过期后会被 etcd **自动删除**。服务注册就依赖这个机制——服务只要还活着就持续续租，死了之后 etcd 会自动清理它的 key。

---

## 整体架构（与 C++ libatapp 完全对齐）

```
EtcdCluster（对应 C++ etcd_cluster）
  │
  ├─ 持有全局唯一 leaseID（keepaliveState.leaseID，对应 C++ conf_.lease）
  │
  ├─ 启动时 ensureClusterLease(ctx, ttl) → Grant(ttl) 一次性申请全局 Lease
  │
  ├─ runClusterKeepalive() goroutine（对应 C++ create_request_lease_keepalive()）
  │      └─ KeepAlive(ctx, leaseID) → 续租成功后调用 km.SetLease(newLeaseID)
  │              └─ 遍历所有 actor 调用 SetLeaseID()（对应 C++ set_lease() 遍历）
  │
  └─ KeepaliveManager 管理所有 EtcdKeepalive actor
         ├─ actor: /basepath/by_id/{id}/       ←┐
         ├─ actor: /basepath/by_name/{name}/   ←┤  全部共享同一个 leaseID
         ├─ actor: /basepath/by_type_id/{tid}/ ←┤
         ├─ actor: /basepath/by_type_name/{tn} ←┤
         └─ actor: /basepath/by_tag/{tag}/     ←┘

Stop():
  ├─ leaseCancel()       → 停止 runClusterKeepalive goroutine
  └─ Revoke(leaseID)     → 撤销全局 Lease → 所有 key 立即删除
```

---

## 整体流程图

```
调用者
  │
  ▼
EtcdCluster.RegisterService()
  │
  ├─ [1] ensureClusterLease(ctx, ttl)   ← 全局只 Grant 一次（double-check 锁）
  │         └─ etcdClient.Grant(ctx, ttl) → leaseID
  │         └─ 启动 runClusterKeepalive(ctx, leaseID) goroutine
  │
  └─ [2] 对每条注册路径：svc.StartWithLease(ctx, clusterLeaseID)
           │
           └─► goroutine: runRegisterLoop()
                    │
                    ├─ [3] runCheckBeforeUpdate()    ← 前置检查（GET 已有 key）
                    │         └─ checkIdentityCollision()
                    │
                    ├─ [4] registerWithLease()       ← 写入 key，使用集群 leaseID
                    │         └─ etcdClient.Put(path, jsonValue, WithLease(leaseID))
                    │
                    └─ [等待 SetLeaseID 通知]        ← 不启动独立续租 goroutine

runClusterKeepalive()（后台持续运行）：
  │
  └─ etcdClient.KeepAlive(ctx, leaseID) → ch
       └─ for { select {
               case ka := <-ch:       ← 续租成功
                   km.SetLease(ka.ID) ← 通知所有 actor 刷新 key
               case <-ctx.Done():     ← leaseCancel() 被调用时停止
           }}

Stop() / Unregister():
  ├─ leaseCancel()            ← 停止 runClusterKeepalive goroutine
  └─ etcdClient.Revoke(leaseID)  ← 主动撤销全局 Lease → 所有 key 立即删除
```

---

## 第一步：申请全局 Lease（`ensureClusterLease`）

**源码位置**：`pkg/cluster/cluster.go`

```go
func (c *EtcdCluster) ensureClusterLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // double-check：已有 lease 则直接复用
    if c.keepalives.leaseID != 0 {
        return c.keepalives.leaseID, nil
    }

    // 全局只 Grant 一次
    resp, err := c.etcdClient.Grant(ctx, ttl)
    if err != nil {
        return 0, err
    }

    c.keepalives.leaseID = resp.ID
    c.keepalives.leaseTTL = ttl

    // 启动全局续租 goroutine
    kaCtx, kaCancel := context.WithCancel(context.Background())
    c.keepalives.leaseCancel = kaCancel
    go c.runClusterKeepalive(kaCtx, resp.ID)

    return resp.ID, nil
}
```

- **只 Grant 一次**：double-check 锁保证并发安全
- 对应 C++ `etcd_cluster.cpp:1504-1532` 的全局 lease 申请逻辑
- TTL 单位：秒，对应 C++ `keepalive_timeout`（默认 16 秒）

---

## 第二步：全局续租（`runClusterKeepalive`）

**源码位置**：`pkg/cluster/cluster.go`

```go
func (c *EtcdCluster) runClusterKeepalive(ctx context.Context, leaseID clientv3.LeaseID) {
    ch, err := c.etcdClient.KeepAlive(ctx, leaseID)
    if err != nil {
        return
    }
    for {
        select {
        case ka, ok := <-ch:
            if !ok {
                return // lease 丢失
            }
            // 续租成功 → 通知所有 actor 刷新 key
            if km := c.keepalivesManager; km != nil {
                km.SetLease(ka.ID)
            }
        case <-ctx.Done():
            return // leaseCancel() 被调用
        }
    }
}
```

- 对应 C++ `etcd_cluster.cpp:1546-1608` 的 `create_request_lease_keepalive()`
- 续租成功后调用 `km.SetLease()`，遍历所有 actor 调用 `SetLeaseID()`
- 对应 C++ `etcd_cluster.cpp:914-936` 的 `set_lease()` 遍历逻辑

---

## 第三步：actor 使用集群 Lease 注册（`StartWithLease`）

**源码位置**：`pkg/keepalive/keepalive.go`

```go
func (s *EtcdKeepalive) StartWithLease(ctx context.Context, leaseID clientv3.LeaseID) error {
    if leaseID == 0 {
        return fmt.Errorf("leaseID is required for cluster registration")
    }

    s.mu.Lock()
    s.isClosed = false
    s.leaseID = leaseID
    s.state = KeepaliveInitializing
    s.valueChanged = true
    s.mu.Unlock()

    return s.registerOnce(ctx)  // 直接用集群 leaseID 写 key，无独立续租 goroutine
}
```

- actor **不**持有 lease、**不**续租、**不**负责 Revoke——一切由 cluster 统一管理
- 对应 C++ `etcd_keepalive.cpp:150` 的 `assign_lease=true` 写 key 逻辑
- `registerOnce()` 直接使用传入的 leaseID 执行 PUT，无需 `grantLease()`

---

## 第四步：actor 接收 lease 更新（`SetLeaseID`）

**源码位置**：`pkg/keepalive/keepalive.go`

```go
func (s *EtcdKeepalive) SetLeaseID(leaseID clientv3.LeaseID) {
    s.mu.Lock()
    wasClosed := s.isClosed
    oldLeaseID := s.leaseID
    s.leaseID = leaseID
    // 只在 lease 真正变化或首次写入时才标记需要刷新
    if !wasClosed && leaseID != 0 && (leaseID != oldLeaseID || s.lastValue == "") {
        s.valueChanged = true
    }
    s.mu.Unlock()
    if !wasClosed {
        _ = s.refresh(context.Background())
    }
}
```

- 对应 C++ `etcd_keepalive::active()` 被 `set_lease()` 调用时的行为
- `valueChanged` 仅在 leaseID 实际发生变化（或首次注册）时置 true，避免无效 PUT
- `KeepaliveManager.SetLease()` 遍历所有 actor 依次调用此方法

---

## 第五步：绑定 Lease 写入 Key（`registerOnce` / `refresh`）

**源码位置**：`pkg/keepalive/keepalive.go`

两个写入路径，逻辑相同，均使用集群 leaseID：

| 场景 | 调用路径 |
|------|---------|
| 首次注册 | `StartWithLease()` → `registerOnce()` → `etcdClient.Put(path, value, WithLease(leaseID))` |
| lease 续期后刷新 | `SetLeaseID()` → `refresh()` → `etcdClient.Put(path, value, WithLease(leaseID))` |

```go
// registerOnce：首次注册，使用集群 leaseID
_, err = s.etcdClient.Put(ctx, s.path, string(jsonValue), clientv3.WithLease(leaseIDCopy))

// refresh：lease 更新后刷新 key，同样使用集群 leaseID
_, err = etcdClient.Put(ctx, path, string(jsonValue), clientv3.WithLease(leaseID))
```

- key 的值是 **protobuf-JSON 格式**（`MarshalDiscoveryToJSON`，与 C++ libatapp 兼容）
- `WithLease(leaseID)` 让这个 key 的生命周期与全局 lease 绑定
- actor **不持有** lease，不负责 Revoke——失败时直接返回错误，由调用方处理

---

## 第六步：注销（`Unregister` / cluster `Stop`）

**actor 的 `Unregister()`**（`pkg/keepalive/keepalive.go`）：

```go
func (s *EtcdKeepalive) Unregister(ctx context.Context) error {
    s.mu.Lock()
    s.isClosed = true
    s.state = KeepaliveStopped
    path := s.path
    s.mu.Unlock()

    // actor 只负责删除自己的 key，不 Revoke lease
    _, err := s.etcdClient.Delete(ctx, path)
    return err
}
```

**cluster 的 `stopInternal()`**（`pkg/cluster/cluster.go`）：

```go
func (c *EtcdCluster) stopInternal(ctx context.Context) {
    // 1. 停止全局续租 goroutine
    if c.keepalives.leaseCancel != nil {
        c.keepalives.leaseCancel()
        c.keepalives.leaseCancel = nil
    }

    // 2. 撤销全局 Lease → etcd 立即删除所有绑定的 key
    if c.keepalives.leaseID != 0 {
        c.etcdClient.Revoke(ctx, c.keepalives.leaseID)
        c.keepalives.leaseID = 0
    }
}
```

- **actor 只 DELETE 自己的 key**，绝不 Revoke lease
- **lease Revoke 由 cluster 统一负责**，一次 Revoke 清除所有绑定的 key
- 对应 C++ `etcd_cluster` 析构时撤销全局 lease 的逻辑

---

## Identity 碰撞检测

**注册前**和**每次值刷新前**，都会先 GET 现有 key，防止覆盖不同实例：

**源码位置**：`pkg/keepalive/keepalive.go`

```go
func (s *EtcdKeepalive) checkIdentityCollision(checkedValue string, currentInfo *protocol.AtappDiscovery) error {
    if err := codec.UnmarshalDiscoveryFromJSON([]byte(checkedValue), &existingDiscovery); err == nil {
        if existingDiscovery.Identity != "" && existingDiscovery.Identity != currentInfo.Identity {
            return fmt.Errorf("identity collision: ...")  // 阻止覆盖
        }
    }
    // 兼容旧格式：base64(proto-bytes) 回退解码
}
```

如果 etcd 中已存在**不同 identity** 的服务，直接报错，**拒绝覆盖**。与 C++ libatapp 的 `etcd_keepalive.cpp` 行为一致。

---

## KeepaliveManager：共享全局 Lease

一个服务实例通常注册在**多个路径**下（按 id / name / type_id / type_name / tag），每条路径各有一个 `EtcdKeepalive` actor，但**全部共享同一个全局 leaseID**：

```
服务实例
  ├─ /basepath/by_id/{id}/          → EtcdKeepalive (共享全局 leaseID)
  ├─ /basepath/by_name/{name}/      → EtcdKeepalive (共享全局 leaseID)
  ├─ /basepath/by_type_id/{tid}/    → EtcdKeepalive (共享全局 leaseID)
  ├─ /basepath/by_type_name/{tn}/   → EtcdKeepalive (共享全局 leaseID)
  └─ /basepath/by_tag/{tag}/        → EtcdKeepalive (共享全局 leaseID)
```

`KeepaliveManager.SetLease()` 遍历所有 actor 调用 `SetLeaseID()`（对应 C++ `set_lease()` 遍历）：

```go
func (m *KeepaliveManager) SetLease(leaseID clientv3.LeaseID) {
    for _, actor := range m.actors {
        actor.SetLeaseID(leaseID)
    }
}
```

`EtcdCluster.GetLease()` 直接返回全局 leaseID：

**源码位置**：`pkg/cluster/cluster.go`

```go
func (c *EtcdCluster) GetLease() int64 {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return int64(c.keepalives.leaseID)
}
```

---

## Keepalive 状态机

```
KeepaliveInitializing
      │
      │ StartWithLease() 注册成功
      ▼
KeepaliveActive       ←──────────────────────────────
      │                                               │
      │ SetLeaseID() 通知 lease 更新                  │ refresh() 成功（重写 key 绑定新 leaseID）
      │ → 调用 refresh()，重写 key                   │
      └───────────────────────────────────────────────┘
      │
      │ Unregister() / cluster.Stop()
      ▼
KeepaliveStopped
```

状态说明：
- `KeepaliveInitializing`：`StartWithLease()` 调用中，PUT 尚未完成
- `KeepaliveActive`：已成功注册，可接收 `SetLeaseID()` 更新
- `KeepaliveFailed`：`StartWithLease()` 失败，`isClosed=true`
- `KeepaliveStopped`：`Unregister()` 完成，已从 etcd 删除 key

---

## C++ libatapp 对应关系

| Go 实现 | C++ 对应 | 说明 |
|---------|---------|------|
| `keepaliveState.leaseID` | `etcd_cluster.conf_.lease` | 全局唯一 leaseID |
| `ensureClusterLease()` | `etcd_cluster.cpp:1504-1532` | 全局 Grant Lease |
| `runClusterKeepalive()` | `create_request_lease_keepalive()` | 全局续租 goroutine |
| `KeepaliveManager.SetLease()` | `etcd_cluster::set_lease()` 遍历逻辑 | 通知所有 actor |
| `EtcdKeepalive.SetLeaseID()` | `etcd_keepalive::active()` | actor 接收 lease 更新 |
| `StartWithLease()` | `assign_lease=true` 写 key | 集群模式注册入口 |
| `Unregister() → Delete(key)` | actor 不持有 lease，只删 key | actor 不负责 Revoke |
| lease Revoke | `etcd_cluster` 析构 / `stopInternal()` | cluster 统一 Revoke |

---

## 关键参数对应关系（Go vs C++ libatapp）

| 参数 | Go 代码位置 | C++ 对应字段 | Go 默认值 | C++ 默认值 |
|------|------------|-------------|-----------|-----------|
| Lease TTL | `keepaliveState.leaseTTL` → `Grant(ctx, ttl)` | `keepalive_timeout` | 调用方指定 | 16 秒 |
| 续租间隔 | etcd SDK 自动（≈ TTL/3） | `keepalive_interval` | 自动 | 5 秒 |
| 注册超时 | `defaultRegisterTimeout = 10s` | — | 10 秒 | — |

---

## 相关源码文件

| 文件 | 说明 |
|------|------|
| `pkg/keepalive/keepalive.go` | `EtcdKeepalive`：`StartWithLease`、`SetLeaseID`、`Unregister`（只 Delete key） |
| `pkg/cluster/cluster.go` | `EtcdCluster`：`ensureClusterLease`、`runClusterKeepalive`、`GetLease`、Revoke |
| `pkg/codec/discovery.go` | `MarshalDiscoveryToJSON` / `UnmarshalDiscoveryFromJSON` |
| `pkg/client/client.go` | etcd 客户端配置与 lease keepalive（TLS、auth、endpoints、heartbeat） |

**C++ 参考**（libatapp，只读）：
- `src/atframe/etcdcli/etcd_cluster.cpp:1504-1532`（全局 Lease Grant）
- `src/atframe/etcdcli/etcd_cluster.cpp:1546-1608`（`create_request_lease_keepalive`）
- `src/atframe/etcdcli/etcd_cluster.cpp:914-936`（`set_lease` 遍历 actor）
- `src/atframe/etcdcli/etcd_keepalive.cpp:150`（`assign_lease=true` 写 key）
