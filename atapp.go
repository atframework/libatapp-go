package libatapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	lu "github.com/atframework/atframe-utils-go/lang_utility"
	log "github.com/atframework/atframe-utils-go/log"
	"google.golang.org/protobuf/proto"

	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/panjf2000/ants/v2"
)

// App 应用模式
type AppMode int

const (
	AppModeCustom AppMode = iota
	AppModeStart
	AppModeStop
	AppModeReload
	AppModeInfo
	AppModeHelp
)

// App 状态标志
type AppFlag uint64

const (
	AppFlagInitializing AppFlag = 1 << iota
	AppFlagInitialized
	AppFlagRunning
	AppFlagStopping
	AppFlagStopped
	AppFlagTimeout
	AppFlagInCallback
	AppFlagInTick
	AppFlagDestroying
)

// App 配置
type AppConfig struct {
	// Runtime配置
	HashCode     string
	AppName      string
	BuildVersion string
	AppVersion   string
	ConfigFile   string
	PidFile      string
	ExecutePath  string

	// 日志配置
	StartupLog       []string
	StartupErrorFile string
	CrashOutputFile  string

	// 文件配置
	ConfigPb         *atframe_protocol.AtappConfigure
	ConfigLog        *atframe_protocol.AtappLog
	ConfigOriginData interface{}
}

// 消息类型
type Message struct {
	Type       int32
	Sequence   uint64
	Data       []byte
	Metadata   map[string]string
	SourceId   uint64
	SourceName string
}

// 事件处理函数类型
type EventHandler func(*AppInstance, *AppActionSender) int

// App 应用接口
type AppImpl interface {
	Run(arguments []string) error

	Init(arguments []string) error
	RunOnce(tickTimer *time.Ticker) error
	Stop() error
	Reload() error

	GetId() uint64
	GetTypeId() uint64
	GetTypeName() string
	GetAppName() string
	GetAppIdentity() string
	GetHashCode() string
	GetAppVersion() string
	GetBuildVersion() string
	GetConfigFile() string

	GetSysNow() time.Time

	AddModule(typeInst lu.TypeID, module AppModuleImpl) error

	GetModule(typeInst lu.TypeID) AppModuleImpl

	// 消息相关
	SendMessage(targetId uint64, msgType int32, data []byte) error
	SendMessageByName(targetName string, msgType int32, data []byte) error

	// 事件相关
	SetEventHandler(eventType string, handler EventHandler)
	TriggerEvent(eventType string, args *AppActionSender) int

	// 自定义Action
	PushAction(callback func(action *AppActionData) error, message_data []byte, private_data interface{}) error

	// 配置相关
	GetConfig() *AppConfig
	LoadConfig(configFile string, configurePrefixPath string, loadEnvironemntPrefix string, existedKeys *ConfigExistedIndex) error
	LoadConfigByPath(target proto.Message,
		configurePrefixPath string, loadEnvironemntPrefix string,
		existedKeys *ConfigExistedIndex, existedSetPrefix string,
	) error
	LoadLogConfigByPath(target *atframe_protocol.AtappLog,
		configurePrefixPath string, loadEnvironemntPrefix string,
		existedKeys *ConfigExistedIndex, existedSetPrefix string,
	) error

	// 状态相关
	IsInited() bool
	IsRunning() bool
	IsClosing() bool
	IsClosed() bool
	CheckFlag(flag AppFlag) bool
	SetFlag(flag AppFlag, value bool) bool

	// 生命周期管理
	GetAppContext() context.Context

	// Logger
	GetDefaultLogger() *log.Logger
	GetLogger(index int) *log.Logger
}

type AppLog struct {
	loggers []*log.Logger
	writers []log.LogWriter
}

type AppInstance struct {
	// 基础配置
	config AppConfig
	flags  uint64 // 原子操作的状态标志
	mode   AppMode

	// 命令行参数处理
	flagSet        *flag.FlagSet
	commandManager *CommandManager
	lastCommand    []string

	// 模块管理
	modules   []AppModuleImpl
	moduleMap map[lu.TypeID]AppModuleImpl

	// 生命周期控制
	appContext    context.Context
	stopAppHandle context.CancelFunc

	// 定时器和事件
	tickTimer     *time.Ticker
	stopTimepoint time.Time
	stopTimeout   time.Time

	// 事件处理
	eventHandlers map[string]EventHandler
	eventMutex    sync.RWMutex

	// 信号处理
	signalChan chan os.Signal

	// 日志缓存数据
	logFrameInfoCache sync.Map

	// 日志
	logger *AppLog

	// 协程池
	workerPool *ants.PoolWithFunc

	// 统计信息
	stats struct {
		lastProcEventCount uint64
		tickCount          uint64
		moduleReloadCount  uint64
	}

	timeOffset time.Duration // 时间偏移

	// Cached parsed id value (避免每次调用 GetId 都执行字符串解析)
	cachedId    atomic.Uint64
	cachedIdStr atomic.Value // stores string; empty means not cached
}

func CreateAppInstance() AppImpl {
	ret := &AppInstance{
		mode:          AppModeHelp,
		moduleMap:     make(map[lu.TypeID]AppModuleImpl),
		stopTimepoint: time.Time{},
		stopTimeout:   time.Time{},
		eventHandlers: make(map[string]EventHandler),
		signalChan:    make(chan os.Signal, 1),
		logger: &AppLog{
			loggers: make([]*log.Logger, 0),
		},
	}

	// 初始化编译信息
	initBuildInfo()

	handler := log.NewLogHandlerImpl(&ret.logFrameInfoCache, "[%P][%L](%k:%n): ")
	handler.AppendWriter(log.CreateLogHandlerWriter(log.NewlogStdoutWriter()))
	ret.logger.loggers = append(ret.logger.loggers, log.NewLogger(handler, ret))

	ret.flagSet = flag.NewFlagSet(
		fmt.Sprintf("%s [options...] <start|stop|reload|run> [<custom command> [command args...]]", filepath.Base(os.Args[0])), flag.ContinueOnError)
	ret.flagSet.Bool("version", false, "print version and exit")
	ret.flagSet.Bool("help", false, "print help and exit")
	ret.flagSet.String("config", "", "config file path")
	ret.flagSet.String("pid", "", "pid file path")
	ret.flagSet.String("startup-log", "", "startup log output <\"stdout,stderr,stdsys,/path/to/file\">")
	ret.flagSet.String("crash-output-file", "", "crash output file")

	ret.appContext, ret.stopAppHandle = context.WithCancel(context.Background())

	// 设置默认配置
	ret.config.ExecutePath = os.Args[0]
	ret.config.AppVersion = "1.0.0"
	ret.config.BuildVersion = fmt.Sprintf("libatapp-go based atapp %s %s", ret.config.AppVersion, GetBuildInfo().ToString())

	runtime.SetFinalizer(ret, func(app *AppInstance) {
		app.destroy()
	})

	// TODO: 内置公共引用层模块

	return ret
}

func (app *AppInstance) destroy() {
	if !app.IsClosed() {
		app.close()
	}

	if app.IsInited() {
		app.cleanup()
	}

	for _, m := range slices.Backward(app.modules) {
		m.OnUnbind()
	}

	// TODO: endpoint 断开连接
	// TODO: connector 清理
}

// 生成哈希码
func (app *AppInstance) generateHashCode() {
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s_%d_%s", app.GetAppName(), app.GetId(), app.config.ExecutePath)))
	app.config.HashCode = hex.EncodeToString(hasher.Sum(nil))
}

// 状态管理方法
func checkFlag(flags uint64, checked AppFlag) bool {
	return flags&uint64(checked) != 0
}

func (app *AppInstance) getFlags() uint64 {
	return atomic.LoadUint64(&app.flags)
}

func (app *AppInstance) CheckFlag(flag AppFlag) bool {
	return checkFlag(app.getFlags(), flag)
}

func (app *AppInstance) SetFlag(flag AppFlag, value bool) bool {
	for {
		old := atomic.LoadUint64(&app.flags)
		var new uint64
		if value {
			new = old | uint64(flag)
		} else {
			new = old &^ uint64(flag)
		}
		if atomic.CompareAndSwapUint64(&app.flags, old, new) {
			return old&uint64(flag) != 0
		}
	}
}

func (app *AppInstance) InitLog(config *atframe_protocol.AtappLog) (*AppLog, error) {
	if config == nil {
		return nil, fmt.Errorf("log config is nil")
	}

	globalLevel := log.ConvertLogLevel(config.Level)
	appLog := new(AppLog)

	for i := range config.Category {
		index := config.Category[i].Index

		handler := log.NewLogHandlerImpl(&app.logFrameInfoCache, config.Category[i].Prefix)
		for sinkIndex := range config.Category[i].Sink {
			writer := log.CreateLogHandlerWriter(nil)
			writer.MinLevel = max(globalLevel, log.ConvertLogLevel(config.Category[i].Sink[sinkIndex].Level.Min))
			writer.MaxLevel = log.ConvertLogLevel(config.Category[i].Sink[sinkIndex].Level.Max)

			if config.Category[i].Sink[sinkIndex].Type == "file" {
				flushInterval := int64(config.Category[i].Sink[sinkIndex].GetLogBackendFile().FlushInterval.Nanos) + config.Category[i].Sink[sinkIndex].GetLogBackendFile().FlushInterval.Seconds*int64(time.Second)
				bufferWriter, err := log.NewLogBufferedRotatingWriter(app,
					config.Category[i].Sink[sinkIndex].GetLogBackendFile().GetFile(),
					config.Category[i].Sink[sinkIndex].GetLogBackendFile().GetWritingAlias(),
					config.Category[i].Sink[sinkIndex].GetLogBackendFile().GetRotate().GetSize(),
					config.Category[i].Sink[sinkIndex].GetLogBackendFile().GetRotate().GetNumber(),
					time.Duration(flushInterval),
					65536)
				if err != nil {
					return nil, err
				}
				writer.Out = bufferWriter
				if config.Category[i].Stacktrace.Min != "disable" {
					writer.EnableStackTrace = true
					writer.StackTraceLevel = log.ConvertLogLevel(config.Category[i].Stacktrace.Min)
				}
				writer.AutoFlushLevel = log.ConvertLogLevel(config.Category[i].Sink[sinkIndex].GetLogBackendFile().AutoFlush)
				handler.AppendWriter(writer)
				appLog.writers = append(appLog.writers, writer.Out)
			}

			if config.Category[i].Sink[sinkIndex].Type == "stdout" {
				writer.Out = log.NewlogStdoutWriter()
				handler.AppendWriter(writer)
				appLog.writers = append(appLog.writers, writer.Out)
			}

			if config.Category[i].Sink[sinkIndex].Type == "stderr" {
				writer.Out = log.NewlogStderrWriter()
				handler.AppendWriter(writer)
				appLog.writers = append(appLog.writers, writer.Out)
			}
		}

		if len(appLog.loggers) <= int(index) {
			appLog.loggers = append(appLog.loggers, make([]*log.Logger, int(index)+1-len(appLog.loggers))...)
		}
		appLog.loggers[index] = log.NewLogger(handler, app)
	}

	for i := range appLog.loggers {
		if appLog.loggers[i] == nil {
			handler := log.NewLogHandlerImpl(&app.logFrameInfoCache, "[%P][%L](%k:%n): ")
			handler.AppendWriter(log.CreateLogHandlerWriter(log.NewlogStdoutWriter()))
			appLog.loggers[i] = log.NewLogger(handler, app)
		}
	}
	return appLog, nil
}

func (app *AppInstance) IsInited() bool  { return app.CheckFlag(AppFlagInitialized) }
func (app *AppInstance) IsRunning() bool { return app.CheckFlag(AppFlagRunning) }
func (app *AppInstance) IsClosing() bool { return app.CheckFlag(AppFlagStopping) }
func (app *AppInstance) IsClosed() bool  { return app.CheckFlag(AppFlagStopped) }

func (app *AppInstance) AddModule(typeInst lu.TypeID, module AppModuleImpl) error {
	if lu.IsNil(module) {
		return fmt.Errorf("module is nil")
	}

	flags := app.getFlags()
	if checkFlag(flags, AppFlagInitialized) || checkFlag(flags, AppFlagInitializing) {
		return fmt.Errorf("cannot add module when app is initializing or initialized")
	}

	app.modules = append(app.modules, module)
	app.moduleMap[typeInst] = module
	module.OnBind()
	return nil
}

func AtappAddModule[ModuleType AppModuleImpl](app AppImpl, module ModuleType) error {
	if lu.IsNil(app) {
		return fmt.Errorf("app is nil")
	}

	return app.AddModule(lu.GetTypeIDOf[ModuleType](), module)
}

func (app *AppInstance) GetModule(typeInst lu.TypeID) AppModuleImpl {
	mod, ok := app.moduleMap[typeInst]
	if !ok {
		return nil
	}

	return mod
}

func AtappGetModule[ModuleType AppModuleImpl](app AppImpl) ModuleType {
	var zero ModuleType
	if lu.IsNil(app) {
		return zero
	}

	ret := app.GetModule(lu.GetTypeIDOf[ModuleType]())
	if ret == nil {
		return zero
	}

	convertRet, ok := ret.(ModuleType)
	if !ok {
		return zero
	}
	return convertRet
}

func (app *AppInstance) Init(arguments []string) error {
	if app.IsInited() {
		return nil
	}

	if app.CheckFlag(AppFlagInitializing) {
		return fmt.Errorf("recursive initialization detected")
	}

	app.SetFlag(AppFlagInitializing, true)
	defer app.SetFlag(AppFlagInitializing, false)

	// 解析命令行参数
	if err := app.setupOptions(arguments); err != nil {
		return fmt.Errorf("setup options failed: %w", err)
	}
	app.setupCommandManager()

	if app.mode == AppModeInfo {
		return nil
	}

	if app.mode == AppModeHelp {
		app.flagSet.PrintDefaults()
		return nil
	}

	// 初始化启动流程日志
	if err := app.setupStartupLog(); err != nil {
		if app.mode == AppModeStart {
			app.writeStartupErrorFile(err)
		}
		return fmt.Errorf("setup startup log failed: %w", err)
	}

	app.GetDefaultLogger().LogWarn("======================== App Initializing(startup log) ========================")

	// 设置信号处理
	if app.mode != AppModeCustom && app.mode != AppModeStop && app.mode != AppModeReload {
		if err := app.setupSignal(); err != nil {
			if app.mode == AppModeStart {
				app.writeStartupErrorFile(err)
			}
			return fmt.Errorf("setup signal failed: %w", err)
		}
	}

	// 加载配置
	if err := app.LoadConfig(app.config.ConfigFile, "atapp", "ATAPP", nil); err != nil {
		if app.mode == AppModeStart {
			app.writeStartupErrorFile(err)
		}
		return fmt.Errorf("load config failed: %w", err)
	}

	if app.config.AppName == "" {
		app.config.AppName = fmt.Sprintf("%s-0x%x", app.GetAppName(), app.GetId())
	}

	// 生成哈希码
	app.generateHashCode()

	if app.mode == AppModeCustom || app.mode == AppModeStop || app.mode == AppModeReload {
		return app.sendLastCommand()
	}

	// 初始化日志
	if err := app.setupLog(); err != nil {
		if app.mode == AppModeStart {
			app.writeStartupErrorFile(err)
		}
		return fmt.Errorf("setup log failed: %w", err)
	}

	app.GetDefaultLogger().LogWarn("======================== App Initializing(setup log) ========================")

	// 设置定时器
	if err := app.setupTickTimer(); err != nil {
		if app.mode == AppModeStart {
			app.writeStartupErrorFile(err)
		}
		return fmt.Errorf("setup timer failed: %w", err)
	}

	// 初始化协程池大小
	var err error
	app.workerPool, err = ants.NewPoolWithFunc(int(app.config.ConfigPb.GetWorkerPool().GetQueueSize()), func(args interface{}) {
		sender, ok := args.(*AppActionSender)
		if !ok {
			app.GetDefaultLogger().LogError("routine pool args type error, shouldn't happen!")
			return
		}
		app.processAction(sender)
	},
		// , ants.WithNonblocking(true)
		ants.WithPanicHandler(func(a any) {
			app.GetDefaultLogger().LogError("Goroutine Panic:", "info", a)
			panic(a)
		}),
	)
	if err != nil {
		if app.mode == AppModeStart {
			app.writeStartupErrorFile(err)
		}
		return err
	}

	initContext, initCancel := context.WithTimeout(app.appContext, app.config.ConfigPb.GetTimer().GetInitializeTimeout().AsDuration())
	defer initCancel()

	// 初始化所有模块
	// Setup phase
	for _, m := range app.modules {
		if initContext.Err() != nil {
			break
		}
		if err := m.Setup(initContext); err != nil {
			if app.mode == AppModeStart {
				app.writeStartupErrorFile(err)
			}
			return fmt.Errorf("module setup failed: %w", err)
		}

		m.Enable()
	}

	// Setup log phase
	for _, m := range app.modules {
		if initContext.Err() != nil {
			break
		}
		if err := m.SetupLog(initContext); err != nil {
			if app.mode == AppModeStart {
				app.writeStartupErrorFile(err)
			}
			return fmt.Errorf("module setup log failed: %w", err)
		}
	}

	// Start reload phase
	for _, m := range app.modules {
		if initContext.Err() != nil {
			break
		}
		if err := m.Reload(); err != nil {
			if app.mode == AppModeStart {
				app.writeStartupErrorFile(err)
			}
			return fmt.Errorf("module reload failed: %w", err)
		}
	}

	// Init phase
	for _, m := range app.modules {
		if initContext.Err() != nil {
			break
		}
		if err := m.Init(initContext); err != nil {
			if app.mode == AppModeStart {
				app.writeStartupErrorFile(err)
			}
			return fmt.Errorf("%s module init failed: %w", m.Name(), err)
		}

		m.Active()
	}

	maybeErr := initContext.Err()
	if maybeErr == nil {
		maybeErr = app.appContext.Err()
	}

	if maybeErr == nil {
		// TODO: evt_on_all_module_inited_(*this);

		app.SetFlag(AppFlagRunning, true)
		app.SetFlag(AppFlagInitialized, true)
		app.SetFlag(AppFlagStopped, false)
		app.SetFlag(AppFlagStopping, false)

		if app.mode == AppModeStart {
			app.writePidFile()
			app.cleanupStartupErrorFile()
		}

		readyMessage := fmt.Sprintf("======================== App Ready ========================\nApp Version: %s\nBuild Version: %s\nExecute Path: %s\nConfig File: %s\nHash Code: %s",
			app.GetAppVersion(),
			app.GetBuildVersion(),
			app.GetConfig().ExecutePath,
			app.GetConfig().ConfigFile,
			app.GetHashCode(),
		)
		app.GetDefaultLogger().LogWarn(readyMessage)

		// Ready phase
		for _, m := range app.modules {
			m.Ready()
		}
	} else {
		app.GetDefaultLogger().LogWarn("======================== App Startup Failed ========================")

		// 失败处理
		app.Stop()

		for app.IsInited() && !app.IsClosed() {
			app.internalRunOnce(app.tickTimer)
		}

		app.writeStartupErrorFile(maybeErr)
	}

	return maybeErr
}

func (app *AppInstance) internalRunOnce(tickTimer *time.Ticker) error {
	if !app.IsInited() && !app.CheckFlag(AppFlagInitializing) {
		return fmt.Errorf("app is not initialized")
	}

	if app.CheckFlag(AppFlagInCallback) {
		return nil
	}

	if app.mode != AppModeCustom && app.mode != AppModeStart {
		return nil
	}

	select {
	case <-app.appContext.Done():
		break
	case sig := <-app.signalChan:
		if sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
			app.GetDefaultLogger().LogInfo("Received signal, stopping...", slog.Any("signal", sig))
			app.Stop()
		}
	case <-tickTimer.C:
		if err := app.tick(); err != nil {
			app.GetDefaultLogger().LogError("Tick error", slog.Any("err", err))
		}
	}

	flags := app.getFlags()
	if checkFlag(flags, AppFlagStopping) && !checkFlag(flags, AppFlagStopped) {
		now := app.GetSysNow()
		if now.After(app.stopTimeout) {
			app.SetFlag(AppFlagTimeout, true)
		}
		forceTimeout := checkFlag(flags, AppFlagTimeout)
		if now.After(app.stopTimepoint) || forceTimeout {
			app.stopTimepoint = now.Add(app.config.ConfigPb.GetTimer().GetStopInterval().AsDuration())
			app.closeAllModules(forceTimeout)
		}
	}

	if checkFlag(flags, AppFlagStopped) && !checkFlag(flags, AppFlagInitialized) {
		app.cleanup()
	}

	return nil
}

func (app *AppInstance) RunOnce(tickTimer *time.Ticker) error {
	return app.internalRunOnce(tickTimer)
}

func (app *AppInstance) Run(arguments []string) error {
	if !app.IsInited() {
		if err := app.Init(arguments); err != nil {
			app.GetDefaultLogger().LogError("App init failed", "err", err)
			return err
		}
	}

	// 主事件循环
	for app.IsInited() && !app.IsClosed() {
		app.internalRunOnce(app.tickTimer)
	}

	return nil
}

func (app *AppInstance) closeAllModules(forceTimeout bool) (bool, error) {
	// 设置停止标志
	allClosed := true
	var err error = nil

	// all modules stop
	for _, m := range slices.Backward(app.modules) {
		if !m.IsActived() {
			continue
		}

		moduleClosed, err := m.Stop()
		if err != nil {
			app.GetDefaultLogger().LogError("Module %s stop failed: %v", m.Name(), err)
			m.Unactive()
		} else if !moduleClosed {
			if forceTimeout {
				m.Timeout()
				m.Unactive()
			} else {
				allClosed = false
			}
		} else {
			m.Unactive()
		}
	}

	if allClosed {
		app.SetFlag(AppFlagStopped, true)
	}

	return allClosed, err
}

func (app *AppInstance) close() (bool, error) {
	allClosed := true
	var err error = nil

	// all modules stop
	for _, m := range slices.Backward(app.modules) {
		if !m.IsActived() {
			continue
		}

		moduleClosed, err := m.Stop()
		if err != nil {
			app.GetDefaultLogger().LogError("Module %s stop failed: %v", m.Name(), err)
			m.Unactive()
		} else if !moduleClosed {
			allClosed = false
		} else {
			m.Unactive()
		}
	}

	return allClosed, err
}

func (app *AppInstance) cleanup() error {
	// all modules cleanup
	for _, m := range slices.Backward(app.modules) {
		if m.IsEnabled() {
			m.Cleanup()
			m.Disable()
		}
	}

	// TODO: cleanup event

	// close tick timer
	if app.tickTimer != nil {
		app.tickTimer.Stop()
	}

	// cleanup pidfile
	app.cleanupPidFile()

	app.SetFlag(AppFlagRunning, false)
	app.SetFlag(AppFlagInitialized, false)

	// TODO: finally callback
	return nil
}

func (app *AppInstance) RunCommand(arguments []string) error {
	// 分发命令处理逻辑
	command := arguments[0]
	args := arguments[1:]

	app.commandManager.ExecuteCommand(app, command, args)
	return nil
}

func (app *AppInstance) sendLastCommand() error {
	if len(app.lastCommand) == 0 {
		app.GetDefaultLogger().LogError("No command to send")
		return fmt.Errorf("no command to send")
	}
	// TODO: 发送远程指令
	return nil
}

func (app *AppInstance) Stop() error {
	if app.IsClosing() {
		return nil
	}
	app.GetDefaultLogger().LogWarn("======================== App Stopping ========================")

	app.stopTimeout = app.GetSysNow().Add(app.config.ConfigPb.GetTimer().GetStopInterval().AsDuration())
	app.SetFlag(AppFlagStopping, true)
	app.stopAppHandle()
	return nil
}

func (app *AppInstance) Reload() error {
	app.GetDefaultLogger().LogWarn("======================== App Reloading ========================")
	// 重新加载配置
	if err := app.LoadConfig(app.config.ConfigFile, "atapp", "ATAPP", nil); err != nil {
		return fmt.Errorf("reload config failed: %w", err)
	}

	// 重新加载所有模块
	for _, m := range app.modules {
		if err := m.Reload(); err != nil {
			return fmt.Errorf("module reload failed: %w", err)
		}
	}

	atomic.AddUint64(&app.stats.moduleReloadCount, 1)
	return nil
}

// Getter methods
func (app *AppInstance) GetId() uint64 {
	idStr := app.config.ConfigPb.GetId()
	if idStr == "" {
		return 0
	}

	// Return cached value if the source string hasn't changed
	if cached, ok := app.cachedIdStr.Load().(string); ok && cached == idStr {
		return app.cachedId.Load()
	}

	// Parse hex (0x...) or decimal string to uint64
	var val uint64
	var err error
	if strings.HasPrefix(idStr, "0x") || strings.HasPrefix(idStr, "0X") {
		val, err = strconv.ParseUint(idStr[2:], 16, 64)
	} else {
		val, err = strconv.ParseUint(idStr, 10, 64)
	}
	if err != nil {
		return 0
	}

	// Cache the result
	app.cachedId.Store(val)
	app.cachedIdStr.Store(idStr)
	return val
}
func (app *AppInstance) GetTypeId() uint64       { return app.config.ConfigPb.GetTypeId() }
func (app *AppInstance) GetTypeName() string     { return app.config.ConfigPb.GetTypeName() }
func (app *AppInstance) GetAppName() string      { return app.config.AppName }
func (app *AppInstance) GetAppIdentity() string  { return app.config.ConfigPb.GetIdentity() }
func (app *AppInstance) GetHashCode() string     { return app.config.HashCode }
func (app *AppInstance) GetAppVersion() string   { return app.config.AppVersion }
func (app *AppInstance) GetBuildVersion() string { return app.config.BuildVersion }
func (app *AppInstance) GetConfig() *AppConfig   { return &app.config }
func (app *AppInstance) GetConfigFile() string   { return app.config.ConfigFile }

func (app *AppInstance) GetSysNow() time.Time {
	// TODO: 使用逻辑时间戳 Timestamp
	return time.Now()
}

func (app *AppInstance) LoadOriginConfigData(configFile string) (err error) {
	if configFile == "" {
		return
	}

	app.GetDefaultLogger().LogInfo("Loading config from", "configFile", configFile)
	var yamlData map[string]interface{}
	yamlData, err = LoadConfigOriginYaml(configFile)
	if err != nil {
		app.GetDefaultLogger().LogError("Load config failed", "error", err)
		return
	}

	app.config.ConfigOriginData = yamlData

	if app.config.ConfigFile == "" {
		app.config.ConfigFile = configFile
	}
	return
}

// 配置管理
func LoadConfigFromOriginDataByPath(logger *log.Logger,
	originData interface{}, target proto.Message,
	configurePrefixPath string, loadEnvironemntPrefix string,
	loadOptions *LoadConfigOptions,
	existedKeys *ConfigExistedIndex, existedSetPrefix string,
) (err error) {
	if target == nil {
		return fmt.Errorf("target is nil")
	}

	if existedKeys == nil {
		existedKeys = CreateConfigExistedIndex()
	}

	if loadEnvironemntPrefix != "" {
		if _, err := LoadConfigFromEnvironemnt(loadEnvironemntPrefix, target, logger,
			loadOptions, existedKeys, existedSetPrefix); err != nil {
			logger.LogError("Load config from environment failed", "error", err,
				"env prefix", loadEnvironemntPrefix, "message_type", target.ProtoReflect().Descriptor().FullName())
		}
	}

	if originData != nil {
		err = LoadConfigFromOriginData(originData, configurePrefixPath, target, logger,
			loadOptions, existedKeys, existedSetPrefix)
		if err != nil {
			logger.LogError("Load config by path failed", "error", err, "path", configurePrefixPath,
				"message_type", target.ProtoReflect().Descriptor().FullName())
			return err
		}
	}

	// 补全Default values
	LoadDefaultConfigMessageFields(target, logger, existedKeys, existedSetPrefix)
	return
}

func (app *AppInstance) LoadConfigByPath(target proto.Message,
	configurePrefixPath string, loadEnvironemntPrefix string,
	existedKeys *ConfigExistedIndex, existedSetPrefix string,
) error {
	if app == nil {
		return fmt.Errorf("app is nil")
	}

	return LoadConfigFromOriginDataByPath(app.GetDefaultLogger(), app.config.ConfigOriginData, target,
		configurePrefixPath, loadEnvironemntPrefix, nil, existedKeys, existedSetPrefix)
}

func (app *AppInstance) LoadLogConfigByPath(target *atframe_protocol.AtappLog,
	configurePrefixPath string, loadEnvironemntPrefix string,
	existedKeys *ConfigExistedIndex, existedSetPrefix string,
) error {
	if app == nil {
		return fmt.Errorf("app is nil")
	}

	if target == nil {
		return fmt.Errorf("target is nil")
	}

	err := LoadConfigFromOriginDataByPath(app.GetDefaultLogger(), app.config.ConfigOriginData, target,
		configurePrefixPath, loadEnvironemntPrefix, &LoadConfigOptions{
			ReorderListIndexByField: "index",
		}, existedKeys, existedSetPrefix)
	if err != nil {
		return err
	}

	if loadEnvironemntPrefix == "" {
		return nil
	}

	// 日志Category的环境变量读取支持转义
	for i, category := range target.Category {
		if category.Name == "" {
			continue
		}
		category.Index = int32(i)

		LoadLogCategoryConfigFromEnvironemnt(fmt.Sprintf("%s_%s", loadEnvironemntPrefix, category.Name), category,
			app.GetDefaultLogger(), existedKeys, fmt.Sprintf("%scategory.%d.", existedSetPrefix, i))

		// Force index existed key
		if existedKeys != nil {
			forceIndexKey := fmt.Sprintf("%scategory.%d.index", existedSetPrefix, i)
			existedKeys.MutableExistedSet()[forceIndexKey] = struct{}{}
		}
	}

	return nil
}

// 配置管理
func (app *AppInstance) LoadConfig(configFile string, configurePrefixPath string,
	loadEnvironemntPrefix string, existedKeys *ConfigExistedIndex,
) (err error) {
	err = app.LoadOriginConfigData(configFile)
	if err != nil {
		return
	}

	if existedKeys == nil {
		existedKeys = CreateConfigExistedIndex()
	}

	configPb := &atframe_protocol.AtappConfigure{}
	err = app.LoadConfigByPath(configPb, configurePrefixPath, loadEnvironemntPrefix, existedKeys, "")
	if err != nil {
		app.GetDefaultLogger().LogError("Load config failed", "error", err)
		return
	}
	app.config.ConfigPb = configPb

	// Invalidate cached id (config may have changed)
	app.cachedIdStr.Store("")

	// 日志配置单独处理
	configLog := &atframe_protocol.AtappLog{}
	var logLoadEnvironemntPrefix string
	if loadEnvironemntPrefix != "" {
		logLoadEnvironemntPrefix = loadEnvironemntPrefix + "_LOG"
	}
	var logConfigurePrefixPath string
	if configurePrefixPath == "" {
		logConfigurePrefixPath = "log"
	} else {
		logConfigurePrefixPath = configurePrefixPath + ".log"
	}
	err = app.LoadLogConfigByPath(configLog, logConfigurePrefixPath, logLoadEnvironemntPrefix, existedKeys, "log.")
	if err != nil {
		app.GetDefaultLogger().LogError("Load log config failed", "error", err)
	} else {
		app.config.ConfigLog = configLog
	}

	return
}

// 消息相关
func (app *AppInstance) SendMessage(targetId uint64, msgType int32, data []byte) error {
	// TODO: 实现消息发送逻辑
	app.GetDefaultLogger().LogDebug("Sending message",
		"targetId", targetId,
		"type", msgType,
		"size", len(data),
	)
	return nil
}

func (app *AppInstance) SendMessageByName(targetName string, msgType int32, data []byte) error {
	// TODO: 实现按名称发送消息逻辑
	app.GetDefaultLogger().LogDebug("Sending message",
		"targetName", targetName,
		"type", msgType,
		"size", len(data),
	)
	return nil
}

// 事件相关
func (app *AppInstance) SetEventHandler(eventType string, handler EventHandler) {
	app.eventMutex.Lock()
	defer app.eventMutex.Unlock()
	app.eventHandlers[eventType] = handler
}

func (app *AppInstance) TriggerEvent(eventType string, args *AppActionSender) int {
	app.eventMutex.RLock()
	handler, exists := app.eventHandlers[eventType]
	app.eventMutex.RUnlock()

	if exists {
		return handler(app, args)
	}
	return 0
}

func (app *AppInstance) GetAppContext() context.Context {
	return app.appContext
}

func (app *AppInstance) GetDefaultLogger() *log.Logger {
	return app.logger.loggers[0]
}

func (app *AppInstance) GetLogger(index int) *log.Logger {
	log := app.logger
	if len(log.loggers) > index {
		return log.loggers[index]
	}
	return log.loggers[0]
}

// 内部辅助方法
func (app *AppInstance) setupOptions(arguments []string) error {
	// 在测试环境中，如果 arguments 为 nil，跳过参数解析
	if arguments == nil {
		return nil
	}

	// 检查是否在测试环境中
	if err := app.flagSet.Parse(arguments); err != nil {
		return err
	}

	if app.flagSet.Lookup("config").Value.String() != "" {
		app.GetDefaultLogger().LogInfo("Found Config")
		app.config.ConfigFile = app.flagSet.Lookup("config").Value.String()
	} else {
		app.GetDefaultLogger().LogInfo("Not Found Config")
	}

	if app.flagSet.Lookup("version").Value.String() == "true" {
		app.mode = AppModeInfo
		println(app.GetBuildVersion())
		return nil
	}

	if app.flagSet.Lookup("help").Value.String() == "true" {
		app.mode = AppModeHelp
		return nil
	}

	if app.flagSet.Lookup("pid").Value.String() != "" {
		app.config.PidFile = app.flagSet.Lookup("pid").Value.String()
	}

	if app.flagSet.Lookup("startup-log").Value.String() != "" {
		app.config.StartupLog = strings.Split(app.flagSet.Lookup("startup-log").Value.String(), ",")
	}

	if app.flagSet.Lookup("crash-output-file").Value.String() != "" {
		app.config.CrashOutputFile = app.flagSet.Lookup("crash-output-file").Value.String()
	}

	// 检查位置参数以确定命令
	args := app.flagSet.Args()
	if len(args) > 0 {
		switch args[0] {
		case "start":
			app.mode = AppModeStart
		case "stop":
			app.mode = AppModeStop
			app.lastCommand = []string{"stop"}
		case "reload":
			app.mode = AppModeReload
			app.lastCommand = []string{"reload"}
		case "run":
			app.mode = AppModeCustom
			app.lastCommand = args[1:]
		case "help":
			app.mode = AppModeHelp
		case "version":
			app.mode = AppModeInfo
		default:
			return fmt.Errorf("unknown command: %s", args[0])
		}
	}

	return nil
}

func (app *AppInstance) setupSignal() error {
	signal.Notify(app.signalChan, syscall.SIGINT, syscall.SIGHUP, syscall.SIGPIPE, syscall.SIGTERM, syscall.SIGQUIT)
	return nil
}

func (app *AppInstance) setupStartupLog() error {
	if len(app.config.StartupLog) > 0 {
		app.GetDefaultLogger().LogInfo("Setting up startup log", "config", app.config.StartupLog)
		app.logger.loggers = nil

		handler := log.NewLogHandlerImpl(&app.logFrameInfoCache, "[%P][%L](%k:%n): ")
		for _, logFile := range app.config.StartupLog {
			switch logFile {
			case "stdout":
				{
					logHandler := log.CreateLogHandlerWriter(log.NewlogStdoutWriter())
					handler.AppendWriter(logHandler)
				}
			case "stderr":
				{
					logHandler := log.CreateLogHandlerWriter(log.NewlogStderrWriter())
					handler.AppendWriter(logHandler)
				}
			case "stdsys":
				{
					// TODO 支持SYS
				}
			default:
				{
					out, _ := log.NewLogBufferedRotatingWriter(app,
						fmt.Sprintf("../log/%s", logFile), "", 50*1024*1024, 1, time.Second*1, 4096)
					handler.AppendWriter(log.CreateLogHandlerWriter(out))
				}
			}
		}
		app.logger.loggers = append(app.logger.loggers, log.NewLogger(handler, app))
	}

	if app.config.CrashOutputFile != "" {

		dir := filepath.Dir(app.config.CrashOutputFile)
		err := os.MkdirAll(dir, 0o755)
		if err != nil {
			app.GetDefaultLogger().LogError("Create crash output dir failed", "file", app.config.CrashOutputFile, "error", err)
			return err
		}

		f, err := os.OpenFile(app.config.CrashOutputFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			app.GetDefaultLogger().LogError("Create crash output file failed", "file", app.config.CrashOutputFile, "error", err)
			return err
		}

		app.GetDefaultLogger().LogInfo("Setting up crash output file", "file", app.config.CrashOutputFile)
		err = debug.SetCrashOutput(f, debug.CrashOptions{})
		if err != nil {
			app.GetDefaultLogger().LogError("Setting up crash output file failed", "file", app.config.CrashOutputFile, "error", err)
			return err
		}
	}

	return nil
}

func (app *AppInstance) setupLog() error {
	if app.config.ConfigLog == nil {
		return fmt.Errorf("setup process error, configure not loaded")
	}
	// 根据配置设置日志
	appLog, err := app.InitLog(app.config.ConfigLog)
	if err != nil {
		return err
	}

	oldLogger := app.logger
	app.logger = appLog
	for i := range oldLogger.writers {
		oldLogger.writers[i].Close()
	}

	return nil
}

func (app *AppInstance) setupTickTimer() error {
	// 定时器在 Run 方法中设置
	if app.tickTimer == nil {
		app.tickTimer = time.NewTicker(app.config.ConfigPb.GetTimer().GetTickInterval().AsDuration())
	} else {
		app.tickTimer.Reset(app.config.ConfigPb.GetTimer().GetTickInterval().AsDuration())
	}
	return nil
}

func (app *AppInstance) tick() error {
	if app.CheckFlag(AppFlagInTick) {
		return nil
	}

	app.SetFlag(AppFlagInTick, true)
	defer app.SetFlag(AppFlagInTick, false)

	atomic.AddUint64(&app.stats.tickCount, 1)

	// 处理模块的tick
	tickContext, cancel := context.WithCancel(app.appContext)
	defer cancel()

	// 调用模块的tick方法
	for _, m := range app.modules {
		if m.IsActived() {
			m.Tick(tickContext)
		}
	}

	return nil
}

// 辅助方法：写入PID文件
func (app *AppInstance) writePidFile() error {
	if app.config.PidFile == "" {
		return nil
	}

	pid := os.Getpid()
	pidData := fmt.Sprintf("%d\n", pid)

	return os.WriteFile(app.config.PidFile, []byte(pidData), 0o644)
}

// 辅助方法：清理PID文件
func (app *AppInstance) cleanupPidFile() error {
	if app.config.PidFile == "" {
		return nil
	}

	if _, err := os.Stat(app.config.PidFile); err == nil {
		return os.Remove(app.config.PidFile)
	}
	return nil
}

// 辅助方法：写入启动失败标记文件
func (app *AppInstance) writeStartupErrorFile(err error) {
	if app.config.StartupErrorFile == "" {
		return
	}

	pidData := fmt.Sprintf("%v\n", err)
	os.WriteFile(app.config.StartupErrorFile, []byte(pidData), 0o644)
}

// 辅助方法：清理启动失败标记文件
func (app *AppInstance) cleanupStartupErrorFile() error {
	if app.config.StartupErrorFile == "" {
		return nil
	}

	return os.Remove(app.config.StartupErrorFile)
}

// 命令处理相关
type CommandHandler func(*AppInstance, string, []string) error

type CommandManager struct {
	commands map[string]CommandHandler
	mutex    sync.RWMutex
}

func NewCommandManager() *CommandManager {
	return &CommandManager{
		commands: make(map[string]CommandHandler),
	}
}

func (cm *CommandManager) RegisterCommand(name string, handler CommandHandler) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.commands[name] = handler
}

func (cm *CommandManager) ExecuteCommand(app *AppInstance, command string, args []string) error {
	cm.mutex.RLock()
	handler, exists := cm.commands[command]
	cm.mutex.RUnlock()

	if !exists {
		handler, exists = cm.commands["@OnError"]
		if exists {
			return handler(app, command, args)
		} else {
			app.GetDefaultLogger().LogError("LogError command executed: %s %v", command, args)
			return nil
		}
	}

	return handler(app, command, args)
}

func (cm *CommandManager) ListCommands() []string {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	commands := make([]string, 0, len(cm.commands))
	for name := range cm.commands {
		commands = append(commands, name)
	}
	return commands
}

// 为 AppInstance 添加命令管理器
func (app *AppInstance) setupCommandManager() {
	// 这里可以添加一个 commandManager 字段到 AppInstance 结构体中
	// 为了简化，这里返回一个新的实例
	cm := NewCommandManager()

	// 注册默认命令（使用包装函数来匹配 CommandHandler 签名）
	cm.RegisterCommand("start", func(app *AppInstance, _command string, args []string) error {
		return app.handleStartCommand(args)
	})
	cm.RegisterCommand("stop", func(app *AppInstance, _command string, args []string) error {
		return app.handleStopCommand(args)
	})
	cm.RegisterCommand("reload", func(app *AppInstance, _command string, args []string) error {
		return app.handleReloadCommand(args)
	})
	cm.RegisterCommand("@OnError", func(app *AppInstance, command string, args []string) error {
		app.GetDefaultLogger().LogError("LogError command executed: %s %v", command, args)
		return nil
	})

	app.commandManager = cm
}

// 默认命令处理器
func (app *AppInstance) handleStartCommand(_args []string) error {
	app.GetDefaultLogger().LogInfo("======================== App start ========================")
	return nil
}

func (app *AppInstance) handleStopCommand(_args []string) error {
	app.GetDefaultLogger().LogInfo("======================== App received stop command ========================")
	return app.Stop()
}

func (app *AppInstance) handleReloadCommand(_args []string) error {
	app.GetDefaultLogger().LogInfo("======================== App received reload command ========================")
	err := app.Reload()
	if err != nil {
		app.GetDefaultLogger().LogError("App reload failed", slog.Any("error", err))
		return err
	}

	err = app.setupLog()
	if err != nil {
		app.GetDefaultLogger().LogError("App reload and log setup failed", slog.Any("error", err))
		return err
	}

	return nil
}

func (app *AppInstance) MakeAction(callback func(action *AppActionData) error, message_data []byte, private_data interface{}) *AppActionSender {
	sender := globalAppActionSenderPool.Get().(*AppActionSender)
	sender.callback = callback
	sender.data.MessageData = message_data
	sender.data.PrivateData = private_data
	sender.data.App = app
	return sender
}

// 辅助方法：写入PID文件
func (app *AppInstance) PushAction(callback func(action *AppActionData) error, message_data []byte, private_data interface{}) error {
	sender := app.MakeAction(callback, message_data, private_data)
	if err := app.workerPool.Invoke(sender); err != nil {
		app.GetDefaultLogger().LogError("failed to invoke action", "err", err)
		return err
	}

	return nil
}

func (app *AppInstance) processAction(sender *AppActionSender) {
	err := sender.callback(&sender.data)
	if err != nil {
		app.GetDefaultLogger().LogError("Action callback error", slog.Any("err", err))
	}

	sender.reset()
	globalAppActionSenderPool.Put(sender)
}

// AppAction对象
type AppActionData struct {
	App         AppImpl
	MessageData []byte
	PrivateData interface{}
}

type AppActionSender struct {
	callback func(action *AppActionData) error
	data     AppActionData
}

func (s *AppActionSender) reset() {
	s.callback = nil
	s.data.App = nil
	s.data.MessageData = nil
	s.data.PrivateData = nil
}

var globalAppActionSenderPool = sync.Pool{
	New: func() interface{} {
		return new(AppActionSender)
	},
}
