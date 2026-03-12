# Build & Code Generation

## Prerequisites

- Go 1.25+
- `protoc` (protobuf compiler)
- `protoc-gen-go` (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`)
- `protoc-gen-mutable`（自定义插件，生成 `*_mutable.pb.go`）
- [go-task](https://taskfile.dev/)（可选但推荐）

## Proto 代码生成

```bash
# 使用 Taskfile
task generate-protocol

# 或直接调用
cd protocol && protoc --go_out=. --mutable_out=. \
  --go_opt=paths=source_relative --mutable_opt=paths=source_relative \
  --proto_path=./ ./atframe/*.proto
```

生成的文件位于 `protocol/atframe/` 目录下：
- `*.pb.go` — 标准 protobuf 生成
- `*_mutable.pb.go` — Mutable/Clone/Merge/Readonly 扩展

## 构建

```bash
go build ./...
```

## 依赖

| 依赖 | 用途 |
| --- | --- |
| `github.com/atframework/atframe-utils-go` | 通用工具库（日志、类型工具） |
| `google.golang.org/protobuf` | Protobuf 运行时 |
| `gopkg.in/yaml.v3` | YAML 配置解析 |
| `github.com/panjf2000/ants/v2` | 协程池 |
| `github.com/stretchr/testify` | 测试断言库 |

## 模块路径

```
github.com/atframework/libatapp-go
```

在主工程中通过 `go.mod` replace 指令引用本地路径：
```
replace github.com/atframework/libatapp-go => ./atframework/libatapp-go
```
