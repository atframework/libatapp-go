# etcd_module 目录渐进迁移计划（强收敛版）

目标：在保证行为与测试稳定的前提下，逐步收敛 `pkg` 暴露面，仅保留必要对外入口。

## 目录规范（当前）

1. 业务实现优先放在 `internal` 或 `etcd_module/*`（非 `pkg`）。
2. `pkg` 仅保留必须对外稳定暴露的包。
3. 仓库内部代码优先引用非 `pkg` 路径。
4. 对外兼容层不是默认要求，只有明确需要对外兼容时才保留。

## 迭代策略

1. 每轮只迁移一个包或一组低耦合文件。
2. 先切换仓库内 import，再删除旧 `pkg` 目录。
3. 每轮执行全量测试回归。
4. 若遇到 Go `internal` 可见性限制，使用 `etcd_module/*` 非 `internal` 落点。

## 迭代记录

### Iteration 1

- 新增目录：
  - `etcd_module/cmd`
  - `etcd_module/internal`
  - `etcd_module/configs`
- 迁移样板：`pkg/topology` -> `internal/topology`
- 兼容策略：早期阶段先保留 `pkg` 兼容层（后续按强收敛策略移除）

### Iteration 2

- 迁移样板：`pkg/codec` -> `internal/codec`
- 兼容策略：早期阶段先保留 `pkg` 兼容层（后续按强收敛策略移除）

### Iteration 3

- 迁移样板：`pkg/consistenthash` -> `internal/consistenthash`
- 兼容策略：早期阶段先保留 `pkg` 兼容层（后续按强收敛策略移除）

### Iteration 4

- 迁移样板：`pkg/module/path_builder` 纯路径拼接逻辑 -> `internal/pathbuilder`
- 兼容策略：`pkg/module` 保留对外方法签名，内部转发到 `internal/pathbuilder`

### Iteration 5

- 迁移样板：`pkg/watcher` 纯解析 helper (`decodeDiscoveryValue`) -> `internal/watcherutil`
- 兼容策略：`pkg/watcher` 保留对外类型与行为，内部调用 `internal/watcherutil`

### Iteration 6

- 迁移样板：对 `codec/topology/consistenthash` 执行强收敛，仓库内部全面切换到 `internal/*`
- 兼容策略：按需求移除 `pkg/codec`、`pkg/topology`、`pkg/consistenthash` 目录（不再保留 pkg 转发层）

### Iteration 7

- 迁移样板：`pkg/events` 整包迁移到 `etcd_module/events`
- 兼容策略：仓库内部统一改用 `etcd_module/events`，并移除 `pkg/events` 目录

### Iteration 8

- 迁移样板：清理无引用 `pkg` 包
- 兼容策略：移除 `pkg/auth`、`pkg/config`（仓库内无依赖）

### Iteration 9

- 迁移样板：`pkg/discovery` 整包迁移到 `etcd_module/discovery`
- 兼容策略：全仓引用切换后移除 `pkg/discovery` 目录

### Iteration 10

- 迁移样板：`pkg/watcher` 整包迁移到 `etcd_module/watcher`
- 兼容策略：全仓引用切换后移除 `pkg/watcher` 目录

### Iteration 11

- 迁移样板：`pkg/keepalive` 整包迁移到 `etcd_module/keepalive`
- 兼容策略：全仓引用切换后移除 `pkg/keepalive` 目录

### Iteration 12

- 迁移样板：`pkg/cluster` 整包迁移到 `etcd_module/cluster`
- 兼容策略：全仓引用切换后移除 `pkg/cluster` 目录

### Iteration 13

- 迁移样板：`pkg/client` 整包迁移到 `etcd_module/client`（含 `mocks`）
- 兼容策略：全仓引用切换后移除 `pkg/client` 目录

### Iteration 14

- 迁移样板：移除 `internal/watcherutil` 薄包装 helper
- 兼容策略：将 `DecodeDiscoveryValue` 合并到 `internal/codec`，`watcher` 直接调用 codec

### Iteration 15

- 迁移样板：`pkg/module` 整包迁移到 `etcd_module/module`
- 兼容策略：全仓引用切换后移除 `pkg/module` 目录

### Iteration 16

- 迁移样板：`pkg/docs` 文档目录迁移到 `etcd_module/docs`
- 兼容策略：迁移文档文件后移除 `pkg/docs` 目录

### Iteration 17

- 迁移样板：对 `etcd_module/*` 继续 `internal` 化可行性审计
- 兼容策略：确认 Go `internal` 可见性边界后，仅对可迁移实现执行下沉

## internal 化边界（当前审计）

以下包被仓库根层文件直接引用，不能直接迁入 `etcd_module/internal/*`（否则会触发 Go `internal` 可见性限制）：

1. `etcd_module/discovery`
2. `etcd_module/events`
3. `etcd_module/keepalive`
4. `etcd_module/module`

以下包当前主要由根层测试引用，若要进一步 `internal` 化，需要先改造测试与公开 facade：

1. `etcd_module/client`（含 `mocks`）
2. `etcd_module/cluster`
3. `etcd_module/watcher`

## 后续候选迁移顺序（建议）

1. 为 `client/cluster/watcher` 设计公开 facade（薄转发），实现下沉到 `internal`
2. 为 `discovery/events/keepalive/module` 设计根层 API 适配层，再评估内部化
