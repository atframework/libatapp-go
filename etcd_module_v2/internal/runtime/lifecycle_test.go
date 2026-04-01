package runtime_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// ── ModuleActorRuntime tests ──────────────────────────────────────────────

func TestModuleRuntime_SpawnAndWait(t *testing.T) {
	rt := runtime.NewModuleRuntime()
	ctx := context.Background()

	var ran int32
	rt.Spawn(ctx, func(_ context.Context) {
		atomic.StoreInt32(&ran, 1)
	})

	waitDone := make(chan struct{})
	go func() { rt.Wait(); close(waitDone) }()

	select {
	case <-waitDone:
		assert.Equal(t, int32(1), atomic.LoadInt32(&ran))
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after goroutine exited")
	}
}

func TestModuleRuntime_CtxCancel_StopsAllGoroutines(t *testing.T) {
	rt := runtime.NewModuleRuntime()
	ctx, cancel := context.WithCancel(context.Background())

	var exitCount int32
	for i := 0; i < 4; i++ {
		rt.Spawn(ctx, func(c context.Context) {
			<-c.Done()
			atomic.AddInt32(&exitCount, 1)
		})
	}

	cancel()

	waitDone := make(chan struct{})
	go func() { rt.Wait(); close(waitDone) }()

	select {
	case <-waitDone:
		assert.Equal(t, int32(4), atomic.LoadInt32(&exitCount))
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after ctx cancel")
	}
}

func TestModuleRuntime_MultipleSpawns(t *testing.T) {
	rt := runtime.NewModuleRuntime()
	ctx, cancel := context.WithCancel(context.Background())

	const n = 10
	var sum int32
	for i := 1; i <= n; i++ {
		v := int32(i)
		rt.Spawn(ctx, func(_ context.Context) {
			atomic.AddInt32(&sum, v)
		})
	}

	cancel()                              // some goroutines may not see it; that's fine
	done := make(chan struct{})
	go func() { rt.Wait(); close(done) }()

	select {
	case <-done:
		// All goroutines exited; sum must equal n*(n+1)/2 = 55
		assert.Equal(t, int32(55), atomic.LoadInt32(&sum))
	case <-time.After(3 * time.Second):
		t.Fatal("Wait timed out")
	}
}
