package libatapp

import (
	"context"
	"time"
)

// App 应用接口
type AppModuleImpl interface {
	GetApp() AppImpl

	Name() string

	// Call this callback when a module is added into atapp for the first time
	OnBind()

	// Call this callback when a module is removed from atapp
	OnUnbind()

	// This callback is called after load configure and before initialization(include log)
	Setup(parent context.Context) error

	// This function will be called after reload and before init
	SetupLog(parent context.Context) error

	// This callback is called to initialize a module
	Init(parent context.Context) error

	// This callback is called after all modules are initialized successfully and the atapp is ready to run
	Ready()

	// This callback is called after configure is reloaded
	Reload() error

	// This callback may be called more than once, when the first return false, or this module will be disabled.
	Stop() (bool, error)

	// This callback only will be call once after all module stopped
	Cleanup()

	// This callback be called if the module can not be stopped even in a long time.
	// After this event, all module and atapp will be forced stopped.
	Timeout()

	// This function will be called in every tick if it's actived. return true when busy.
	Tick(parent context.Context) bool

	IsActived() bool
	Active()
	Unactive()

	IsEnabled() bool
	Enable()
	Disable()
}

type noCopy struct{}

type AppModuleBase struct {
	_       noCopy
	owner   AppImpl
	actived bool
	enabled bool
}

func CreateAppModuleBase(owner AppImpl) AppModuleBase {
	return AppModuleBase{
		owner:   owner,
		actived: false,
		enabled: false,
	}
}

func (m *AppModuleBase) GetApp() AppImpl {
	return m.owner
}

func (m *AppModuleBase) OnBind() {}

func (m *AppModuleBase) OnUnbind() {}

func (m *AppModuleBase) Setup(_initCtx context.Context) error {
	return nil
}

func (m *AppModuleBase) SetupLog(_initCtx context.Context) error {
	return nil
}

func (m *AppModuleBase) Ready() {}

func (m *AppModuleBase) Reload() error {
	return nil
}

func (m *AppModuleBase) Stop() (bool, error) {
	return true, nil
}

func (m *AppModuleBase) Cleanup() {}

func (m *AppModuleBase) Timeout() {}

func (m *AppModuleBase) Tick(_initCtx context.Context) bool {
	return false
}

func (m *AppModuleBase) IsActived() bool {
	return m.actived
}

func (m *AppModuleBase) Active() {
	m.actived = true
}

func (m *AppModuleBase) Unactive() {
	m.actived = false
}

func (m *AppModuleBase) IsEnabled() bool {
	return m.enabled
}

func (m *AppModuleBase) Enable() {
	m.enabled = true
}

func (m *AppModuleBase) Disable() {
	m.enabled = false
}

func (m *AppModuleBase) GetSysNow() time.Time {
	return m.owner.GetSysNow()
}
