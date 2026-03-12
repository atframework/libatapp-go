# libatapp-go

A Go implementation of the atapp application framework, inspired by the C++ libatapp.

## Features

- **Application Lifecycle Management**: Complete application lifecycle with Init, Run, Stop, and Reload phases
- **Module System**: Plugin-like architecture for extensible functionality
- **Event System**: Event-driven architecture with custom event handlers
- **Configuration Management**: Command-line flag parsing and configuration file support
- **Signal Handling**: Graceful shutdown on OS signals (SIGINT, SIGTERM)
- **Concurrency Safe**: Thread-safe operations using mutexes and atomic operations
- **Logging**: Built-in logging with configurable levels
- **Performance Optimized**: Efficient implementation with minimal allocations

## Quick Start

### Prepare

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install "github.com/bufbuild/buf/cmd/buf@latest"

# go generate ./...
buf generate
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
    atapp "github.com/atframework/libatapp-go"
)

// Custom module example
type MyModule struct {
    name string
}

func (m *MyModule) OnBind(app atapp.AppInstance) error {
    fmt.Printf("Module %s bound to application\n", m.name)
    return nil
}

func (m *MyModule) Setup(app atapp.AppInstance) int {
    fmt.Printf("Module %s setup complete\n", m.name)
    return 0
}

func (m *MyModule) Init(app atapp.AppInstance) int {
    fmt.Printf("Module %s initialized\n", m.name)
    return 0
}

func (m *MyModule) Ready(app atapp.AppInstance) int {
    fmt.Printf("Module %s ready\n", m.name)
    return 0
}

func (m *MyModule) Stop(app atapp.AppInstance) int {
    fmt.Printf("Module %s stopped\n", m.name)
    return 0
}

func (m *MyModule) Cleanup(app atapp.AppInstance) int {
    fmt.Printf("Module %s cleaned up\n", m.name)
    return 0
}

func main() {
    // Create application instance
    app := atapp.CreateAppInstance()
    
    // Add custom module
    module := &MyModule{name: "MyCustomModule"}
    atapp.AtappAddModule[*MyModule](app, module)
    
    // Set custom event handler
    app.SetEventHandler("custom_event", func(app *atapp.AppInstance, args ...interface{}) int {
        fmt.Printf("Custom event triggered with args: %v\n", args)
        return 0
    })
    
    // Initialize and run application
    if ret := app.Init(nil); ret != 0 {
        log.Fatalf("Application initialization failed with code: %d", ret)
    }
    
    // Trigger custom event
    app.TriggerEvent("custom_event", "hello", "world")
    
    // Run application (blocks until shutdown signal)
    ret := app.Run([]string{}, func() int {
        fmt.Println("Application running...")
        return 0
    })
    
    fmt.Printf("Application exited with code: %d\n", ret)
}
```

### Configuration Management

```go
// Set configuration values
app.GetConfig().SetConfFile("config.yaml")
app.GetConfig().SetVersion("1.0.0")
app.GetConfig().SetAppID(12345)
app.GetConfig().SetTypeID(67890)

// Get configuration values
fmt.Printf("App ID: %d\n", app.GetConfig().GetId())
fmt.Printf("Version: %s\n", app.GetConfig().GetVersion())
```

### Command Handling

```go
// Built-in commands
app.ProcessCommand("version")    // Show version info
app.ProcessCommand("help")       // Show help
app.ProcessCommand("stop")       // Graceful shutdown
app.ProcessCommand("reload")     // Reload configuration
```

## Architecture

The framework follows a modular architecture pattern:

1. **AppInstance**: Main application container managing lifecycle and modules
2. **AppModuleImpl**: Interface for pluggable modules with lifecycle callbacks
3. **AppConfig**: Configuration management with flag parsing
4. **Event System**: Asynchronous event handling with custom handlers
5. **Signal Processing**: OS signal handling for graceful shutdown

## Performance

The implementation is optimized for performance:

- **App Creation**: ~1150 ns/op with minimal allocations
- **Event Triggering**: ~20 ns/op 
- **Module Operations**: Efficient module management
- **Tick Processing**: Low-latency periodic operations

## Testing

Run the test suite:

```bash
go test -v
```

Run benchmarks:

```bash
go test -bench=. -benchmem
```

## TODO List (Future Enhancements)

- [ ] ✅ app层管理和module结构 (Completed)
- [ ] 协议和配置管理
  - [ ] 服务发现配置
  - [ ] 动态日志模块
  - [ ] 通信层配置
- [ ] connector抽象和接入层实现
  - [ ] libatbus-go connector
  - [ ] 本地回环 connector
  - [ ] endpoint管理和消息缓存
- [ ] etcd模块
  - [ ] 服务发现模块接入
- [ ] 策略路由

## License

This project follows the same license as the original libatapp C++ implementation.
