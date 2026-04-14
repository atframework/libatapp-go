package libatapp_types

import (
	"context"
	"time"

	lu "github.com/atframework/atframe-utils-go/lang_utility"
	log "github.com/atframework/atframe-utils-go/log"
	"google.golang.org/protobuf/proto"

	atframe_protocol "github.com/atframework/libatapp-go/protocol/atframe"
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
	IdCmd        string // from -id command line argument

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
type EventHandler func(AppImpl, *AppActionSender) int

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

type AppLog interface {
	GetLoggers() []*log.Logger
	GetWriters() []log.LogWriter
}

// AppAction对象
type AppActionData struct {
	App         AppImpl
	MessageData []byte
	PrivateData interface{}
}

type AppActionSender struct {
	Callback func(action *AppActionData) error
	Data     AppActionData
}

func (s *AppActionSender) Reset() {
	s.Callback = nil
	s.Data.App = nil
	s.Data.MessageData = nil
	s.Data.PrivateData = nil
}
