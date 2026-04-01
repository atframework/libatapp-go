package runtime

import (
	"context"
	"sync"
)

// ModuleActorRuntime manages the goroutine lifetime of all actors within a
// single EtcdModule instance.
//
//   - Spawn must be called before any goroutine it manages starts.
//   - Wait blocks until every spawned goroutine has returned.
type ModuleActorRuntime struct {
	mu sync.Mutex
	wg sync.WaitGroup
}

// NewModuleRuntime creates an idle runtime.
func NewModuleRuntime() *ModuleActorRuntime {
	return &ModuleActorRuntime{}
}

// Spawn launches fn as a managed goroutine.
// ctx should be derived from the module's run context; fn must honour ctx.Done().
func (r *ModuleActorRuntime) Spawn(ctx context.Context, fn func(ctx context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		fn(ctx)
	}()
}

// Wait blocks until all goroutines spawned via Spawn have returned.
func (r *ModuleActorRuntime) Wait() {
	r.wg.Wait()
}
