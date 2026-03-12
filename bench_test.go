package libatapp

import (
	"testing"
)

func BenchmarkAppCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		app := CreateAppInstance()
		_ = app
	}
}

func BenchmarkTick(b *testing.B) {
	app := CreateAppInstance().(*AppInstance)
	app.Init(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app.tick()
	}
}

// func BenchmarkModuleAdd(b *testing.B) {
// 	app := CreateAppInstance().(*AppInstance)

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		module := NewExampleModule("benchmark")
// 		AtappAddModule(app, module)
// 	}
// }

func BenchmarkEventTrigger(b *testing.B) {
	app := CreateAppInstance().(*AppInstance)
	app.SetEventHandler("test", func(app *AppInstance, args *AppActionSender) int {
		return 0
	})

	b.ResetTimer()
	action := app.MakeAction(func(action *AppActionData) error {
		return nil
	}, nil, nil)
	for i := 0; i < b.N; i++ {
		app.TriggerEvent("test", action)
	}
}
