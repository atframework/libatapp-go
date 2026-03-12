# Testing — libatapp-go

## 测试框架

- Go 标准 `testing` 包
- `github.com/stretchr/testify/assert` 断言库

## 运行测试

```bash
cd atframework/libatapp-go
go test ./...

# 带详细输出
go test -v ./...

# 运行特定测试
go test -v -run "TestExpandExpression" ./...
go test -v -run "TestConfigManagement" ./...
```

## 测试文件约定

| 文件                                      | 内容                       |
| ----------------------------------------- | -------------------------- |
| `config_test.go`                          | 配置加载与表达式展开测试   |
| `bench_test.go`                           | 性能基准测试               |
| `atapp_configure_loader_test.yaml`        | YAML 配置加载测试数据      |
| `atapp_configure_loader_test.env.txt`     | 环境变量配置加载测试数据   |
| `atapp_configure_expression_test.yaml`    | 表达式展开 YAML 测试数据   |
| `atapp_configure_expression_test.env.txt` | 表达式展开环境变量测试数据 |
| `atapp_configure_loader_test_*_keys.txt`  | 配置键存在性验证数据       |

## 测试规范

遵循主工程 `/.github/instructions/gotest.instructions.md` 中的规范：

1. **命名**：`Test[Function][Scenario]`
2. **结构**：Arrange-Act-Assert (AAA) 模式
3. **隔离**：每个测试独立，通过 `os.Setenv` / `os.Unsetenv` + `defer` 管理环境变量
4. **覆盖**：包含正常路径、边界条件、错误情况
