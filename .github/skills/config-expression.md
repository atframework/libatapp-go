# Configuration Loading & Expression Expansion

## 概述

libatapp-go 的配置加载系统支持从 YAML 文件和环境变量两个来源加载 protobuf 配置。
对于 proto 字段元数据中标记了 `enable_expression: true` 的字段，支持环境变量表达式展开。

## 配置加载方式

### YAML 配置文件

创建 YAML 文件，结构对应 proto message 字段名（snake_case）：

```yaml
atapp:
  id: "0x00001234"
  bus:
    listen: "atcp://:::21437"
    gateways:
      - address: "tcp://0.0.0.0:8080"
        match_labels:
          region: "us-east-1"
          tier: "frontend"
    receive_buffer_size: 8MB    # 支持 KB/MB/GB 等大小单位
    ping_interval: 60s          # 支持 s/ms/m/h 等时间单位
```

### 环境变量

环境变量名规则：`{PREFIX}_{FIELD_NAME}`，全大写、下划线分隔：

```bash
# 标量字段
ATAPP_ID=0x00001234
ATAPP_BUS_LISTEN=atcp://:::21437

# 数组字段（下标从 0 开始）
ATAPP_BUS_GATEWAYS_0_ADDRESS=tcp://0.0.0.0:8080
ATAPP_BUS_GATEWAYS_0_MATCH_NAMESPACES_0=default

# Map 字段（key/value 分开）
ATAPP_BUS_GATEWAYS_0_MATCH_LABELS_0_KEY=region
ATAPP_BUS_GATEWAYS_0_MATCH_LABELS_0_VALUE=us-east-1
```

### 加载优先级

1. 环境变量（先加载）
2. YAML 文件（后加载，覆盖环境变量的值）
3. Proto 默认值（最后填充未设置的字段）

## 表达式展开语法

在 proto 字段上设置 `enable_expression: true` 后，该字段的字符串值支持以下表达式：

| 语法 | 说明 | 示例 |
| --- | --- | --- |
| `$VAR` | POSIX 标准变量引用 | `$HOME` → `/home/user` |
| `${VAR}` | 花括号变量引用，支持 `.` `-` `/` 等字符 | `${app.kubernetes.io/name}` → `my-app` |
| `${VAR:-default}` | 变量未设置或为空时使用默认值 | `${PORT:-8080}` → `8080` |
| `${VAR:+word}` | 变量已设置且非空时使用 word，否则为空 | `${DEBUG:+--verbose}` |
| `\$` | 转义，输出字面 `$` | `\$HOME` → `$HOME` |
| `${OUTER_${INNER}}` | 嵌套变量名 | 先解析 INNER，再查找拼接后的变量 |
| `${A:-${B:-default}}` | 多级嵌套默认值 | A 未设 → B 未设 → `default` |

### 变量名字符规则

- **`$VAR`（无花括号）**：仅 POSIX 标准 `[a-zA-Z_][a-zA-Z0-9_]*`
- **`${VAR}`（花括号）**：支持 `.` `-` `/` `_` 及字母数字，覆盖 k8s label key 全字符集

### 深度限制

最大嵌套深度 32 层。超过后原样返回，防止无限递归。

### 在 YAML 中使用表达式

```yaml
atapp:
  bus:
    listen: "${BUS_LISTEN:-atcp://:::21437}"
    gateways:
      - address: "${GATEWAY_PROTO:-https}://${GATEWAY_HOST}:${GATEWAY_PORT:-443}"
        match_namespaces:
          - "${K8S_NAMESPACE:-default}"
        match_labels:
          # Map 的 key 和 value 都支持表达式
          "${LABEL_KEY:-env}": "${LABEL_VAL:-production}"
          "region": "${REGION}"
      - address: "${FALLBACK_ADDR:-tcp://0.0.0.0:8080}"
```

### 在环境变量中使用表达式

```bash
# 环境变量的值本身也可以包含表达式
export GATEWAY_HOST=example.com
export GATEWAY_PORT=8443
export ATAPP_BUS_GATEWAYS_0_ADDRESS="${GATEWAY_HOST}:${GATEWAY_PORT}"

# 多级嵌套默认值
export ATAPP_BUS_GATEWAYS_1_ADDRESS="${PRIMARY_ADDR:-${FALLBACK_ADDR:-ws://localhost:9090}}"

# Map field
export ATAPP_BUS_GATEWAYS_0_MATCH_LABELS_0_KEY="${LABEL_KEY:-service}"
export ATAPP_BUS_GATEWAYS_0_MATCH_LABELS_0_VALUE="${LABEL_VAL:-gateway}"
```

## Map 字段的表达式支持

Map 字段的 key/value 是否展开表达式取决于**父 map 字段**上的 `enable_expression` 设置。
例如 proto 中 `match_labels` 字段设置了 `enable_expression: true`，则其所有 key 和 value 都会展开。

## 配置使用注意事项

- getter 可能返回 nil，总是做 nil-check。
- 不要跨 RPC/reload 缓存 config struct 指针（配置可刷新）。
- `GetId()` 返回 `uint64`，内部缓存解析结果，reload 后自动失效。

## 开发注意事项

1. **两条路径都要改**：新增配置加载功能时，YAML 路径和环境变量路径都需要对应修改。
2. **Map 表达式通过父字段控制**：map 的 key/value 本身没有 proto options，需检查父 map 字段的 `enable_expression`。
3. **`ParsePlainMessage` 是独立函数**：不在 `LoadConfig` 流程中被调用，向配置加载添加功能时不要只改它。
4. **YAML 解析类型转换**：YAML 解析器可能将 `0x1234` 等值解析为整数而非字符串，框架内部已做兜底转换。
