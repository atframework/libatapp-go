package runtime_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// ── ActorBase[T] tests ────────────────────────────────────────────────────

func TestActorBase_PostAndReceive(t *testing.T) {
	a := runtime.NewActorBase[int](8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Post messages before starting RunLoop so they are already in the mailbox.
	require.True(t, a.Post(1))
	require.True(t, a.Post(2))
	require.True(t, a.Post(3))
	require.True(t, a.Post(-1)) // sentinel: signals end of work

	var received []int
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.RunLoop(ctx, func(v int) {
			if v == -1 {
				cancel() // all messages received; stop the loop
				return
			}
			received = append(received, v)
		})
	}()

	<-done
	assert.Equal(t, []int{1, 2, 3}, received)
}

func TestActorBase_NonBlockingWhenFull(t *testing.T) {
	// Capacity 2: fill it then verify subsequent Post returns false without blocking.
	a := runtime.NewActorBase[int](2)

	assert.True(t, a.Post(1))
	assert.True(t, a.Post(2))
	// Mailbox full – must not block and must return false.
	assert.False(t, a.Post(3))
}

func TestActorBase_PostCtx_CancelledWhenFull(t *testing.T) {
	a := runtime.NewActorBase[int](1)
	require.True(t, a.Post(99)) // fill mailbox

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := a.PostCtx(ctx, 100)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestActorBase_RunLoopStopsOnCtxCancel(t *testing.T) {
	a := runtime.NewActorBase[int](4)
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	go func() {
		close(started)
		a.RunLoop(ctx, func(int) {})
	}()
	<-started
	cancel()

	select {
	case <-a.Done():
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not exit after ctx cancel")
	}
}

func TestActorBase_Wait_UnblocksAfterRunLoop(t *testing.T) {
	a := runtime.NewActorBase[int](4)
	ctx, cancel := context.WithCancel(context.Background())

	go a.RunLoop(ctx, func(int) {})
	cancel()

	// Wait must return promptly after RunLoop exits.
	waitDone := make(chan struct{})
	go func() {
		a.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock in time")
	}
}

func TestActorBase_PostCtx_EnqueuesBeforeCancel(t *testing.T) {
	a := runtime.NewActorBase[int](4)
	ctx := context.Background()

	err := a.PostCtx(ctx, 42)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	var got int32
	go a.RunLoop(runCtx, func(v int) {
		atomic.StoreInt32(&got, int32(v))
		cancel()
	})
	a.Wait()
	assert.Equal(t, int32(42), atomic.LoadInt32(&got))
}
