# Copilot Instructions for libatapp-go

Go 实现的 atapp 应用框架库，提供应用生命周期管理、模块系统、配置加载与表达式展开等核心功能。
可作为独立组件使用（`github.com/atframework/libatapp-go`）。

## 优先阅读（Skills/Playbooks）

将可复用的工作流说明集中在 `.github/skills/`，本文件保持"短、准、只放关键规则"。

- 构建与代码生成：`.github/skills/build.md`
- 配置加载与表达式展开：`.github/skills/config-expression.md`
- 架构与常见模式：`.github/skills/architecture.md`
- 测试：`.github/skills/testing.md`

## 关键规则（高优先级）

### 配置加载

- Proto 字段元数据通过 `atapp_configure_meta` 扩展控制行为（`enable_expression`、`default_value`、`size_mode` 等）。
- 配置 getter 可能返回 nil；总是做 nil-check。
- 不要跨 RPC/reload 缓存 config struct 指针（配置可刷新）。
- `GetId()` 返回 `uint64`（内部缓存解析结果），config reload 后自动失效缓存。

### 表达式展开

- `$VAR` 无花括号形式仅支持 POSIX 标准字符 `[a-zA-Z_][a-zA-Z0-9_]*`。
- `${VAR}` 花括号形式支持扩展字符（`.` `-` `/` 等），覆盖 k8s label key 全字符集。
- 支持 `${VAR:-default}`、`${VAR:+word}`、`\$` 转义、嵌套 `${OUTER_${INNER}}`、多级默认值 `${OUTER:-${INNER:-default}}`。
- map 字段的 key 和 value 均支持表达式展开（通过父 map 字段的 `enable_expression` 控制）。
- 表达式最大嵌套深度 32 层（`maxExpressionDepth`）。

### 代码风格

- 使用 Go 标准 `testing` 包 + `github.com/stretchr/testify/assert`。
- 测试遵循 AAA 模式（Arrange-Act-Assert）。
- Proto 生成使用 `protoc-gen-go` + 自定义 `protoc-gen-mutable` 插件。
- 日志使用 `github.com/atframework/atframe-utils-go/log` 包。

### 构建

- 优先使用 `task`（`Taskfile.yml` 为准）。
- 生成协议代码：`task generate-protocol`。
- 运行测试：`go test ./...`。
