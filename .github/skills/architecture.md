# Architecture — libatapp-go

## 模块概览

libatapp-go 是 atapp 应用框架的 Go 实现，提供：

- **应用生命周期管理**：Init → Run → Stop → Reload
- **模块系统**：插件式架构，通过 `AppModuleImpl` 接口扩展功能
- **事件系统**：基于事件驱动的处理器注册
- **配置管理**：多源配置加载（YAML + 环境变量），支持表达式展开
- **信号处理**：优雅停止（SIGINT / SIGTERM）
- **协程池**：基于 `ants` 的 worker pool

## 核心文件

| 文件 | 职责 |
| --- | --- |
| `atapp.go` | `AppInstance` 定义、生命周期管理、配置加载入口 |
| `atapp_module_impl.go` | 模块接口定义 |
| `config.go` | 配置加载核心（YAML/环境变量解析、表达式展开、proto field 映射） |
| `build_info.go` | 构建信息 |
| `generate.go` | `go generate` 指令 |
| `protocol/atframe/` | Proto 定义与生成代码 |

## 配置数据流

```
YAML 文件 + 环境变量
        │
        ▼
  LoadConfig()
        │
        ├─ 环境变量 → dumpEnvironemntIntoMessage()
        │      └─ 按 FIELD_NAME 大写 + _ 分隔读取 os.Getenv 的值
        │
        ├─ YAML    → dumpYamlIntoMessage()
        │      └─ 递归遍历 proto 字段描述符，从 yaml map 提取值
        │
        └─ 默认值  → LoadDefaultConfigMessageFields()
               └─ 从 proto field options 的 default_value 取值
```

## Proto 扩展元数据

`atapp_configure_meta`（通过 `CONFIGURE` field option 附加到 proto 字段）控制配置行为：

| 字段 | 类型 | 用途 |
| --- | --- | --- |
| `enable_expression` | bool | 启用环境变量表达式展开 |
| `default_value` | string | 字段默认值 |
| `size_mode` | bool | 值按大小单位解析（KB/MB/GB） |
| `min_value` / `max_value` | string | 值范围约束 |
| `field_match` | message | 条件匹配（同层级 oneof-like 展开） |

## 依赖关系

```
libatapp-go
  ├── atframe-utils-go    (日志、类型工具)
  ├── protobuf            (proto 运行时)
  ├── yaml.v3             (YAML 解析)
  └── ants/v2             (协程池)
```
