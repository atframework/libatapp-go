package runtime_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// helper: build a minimal EventEnvelope with the given type.
func makeEnv(t runtime.EventType) runtime.EventEnvelope {
	return runtime.EventEnvelope{
		Type:       t,
		Version:    1,
		OccurredAt: time.Now(),
	}
}

// ── EventBus tests ────────────────────────────────────────────────────────

func TestEventBus_SubscribeAndPublish(t *testing.T) {
	bus := runtime.NewEventBus()
	const evType runtime.EventType = 7

	var got []runtime.EventEnvelope
	bus.Subscribe(func(e runtime.EventEnvelope) { got = append(got, e) })

	bus.Publish(makeEnv(evType))
	bus.Publish(makeEnv(evType))

	require.Len(t, got, 2)
	assert.Equal(t, evType, got[0].Type)
}

func TestEventBus_SubscribeType_FiltersCorrectly(t *testing.T) {
	bus := runtime.NewEventBus()
	const targetType runtime.EventType = 5

	var count int
	bus.SubscribeType(targetType, func(runtime.EventEnvelope) { count++ })

	bus.Publish(makeEnv(3))          // should NOT trigger
	bus.Publish(makeEnv(targetType)) // should trigger
	bus.Publish(makeEnv(targetType)) // should trigger
	bus.Publish(makeEnv(9))          // should NOT trigger

	assert.Equal(t, 2, count)
}

func TestEventBus_Unsubscribe_RemovesHandler(t *testing.T) {
	bus := runtime.NewEventBus()
	const evType runtime.EventType = 2

	var count int
	handle := bus.Subscribe(func(runtime.EventEnvelope) { count++ })

	bus.Publish(makeEnv(evType)) // +1
	bus.Unsubscribe(handle)
	bus.Publish(makeEnv(evType)) // should NOT trigger

	assert.Equal(t, 1, count)
}

func TestEventBus_MultipleSubscribers_AllCalled(t *testing.T) {
	bus := runtime.NewEventBus()
	const evType runtime.EventType = 1

	var mu sync.Mutex
	var calls []string

	bus.Subscribe(func(runtime.EventEnvelope) {
		mu.Lock()
		calls = append(calls, "A")
		mu.Unlock()
	})
	bus.Subscribe(func(runtime.EventEnvelope) {
		mu.Lock()
		calls = append(calls, "B")
		mu.Unlock()
	})

	bus.Publish(makeEnv(evType))

	assert.Len(t, calls, 2)
}

func TestEventBus_Close_MakesPublishNoop(t *testing.T) {
	bus := runtime.NewEventBus()
	var count int
	bus.Subscribe(func(runtime.EventEnvelope) { count++ })

	bus.Publish(makeEnv(1)) // fires once
	bus.Close()
	bus.Publish(makeEnv(1)) // noop after Close

	assert.Equal(t, 1, count)
}

func TestEventBus_Subscribe_AfterClose_ReturnsZero(t *testing.T) {
	bus := runtime.NewEventBus()
	bus.Close()

	handle := bus.Subscribe(func(runtime.EventEnvelope) {})
	assert.Equal(t, runtime.EventHandleHandle(0), handle)
}

func TestEventBus_SubscribeType_WildcardZero_MatchesAll(t *testing.T) {
	bus := runtime.NewEventBus()

	// EventType(0) is the zero/unknown value which in our enum means wildcard.
	// Subscribe() uses it implicitly; SubscribeType(0, ...) should also match all.
	var count int
	bus.SubscribeType(0, func(runtime.EventEnvelope) { count++ })

	bus.Publish(makeEnv(1))
	bus.Publish(makeEnv(5))
	bus.Publish(makeEnv(9))

	assert.Equal(t, 3, count)
}

// ── Sequence tests ────────────────────────────────────────────────────────

func TestEventBus_Sequence_MonotonicallyIncreasing(t *testing.T) {
	bus := runtime.NewEventBus()

	var seqs []uint64
	bus.Subscribe(func(e runtime.EventEnvelope) {
		seqs = append(seqs, e.Sequence)
	})

	bus.Publish(makeEnv(1))
	bus.Publish(makeEnv(2))
	bus.Publish(makeEnv(3))

	require.Len(t, seqs, 3)
	assert.Less(t, seqs[0], seqs[1], "sequence must increase between publishes")
	assert.Less(t, seqs[1], seqs[2], "sequence must increase between publishes")
}

func TestEventBus_Sequence_StartsAtOne(t *testing.T) {
	bus := runtime.NewEventBus()

	var first uint64
	bus.Subscribe(func(e runtime.EventEnvelope) {
		if first == 0 {
			first = e.Sequence
		}
	})

	bus.Publish(makeEnv(1))
	assert.Equal(t, uint64(1), first)
}

func TestEventBus_Sequence_GlobalAcrossTypes(t *testing.T) {
	// Sequence increments globally regardless of EventType — all events
	// on the same bus share the same counter.
	bus := runtime.NewEventBus()

	var seqs []uint64
	bus.Subscribe(func(e runtime.EventEnvelope) {
		seqs = append(seqs, e.Sequence)
	})

	bus.Publish(makeEnv(1))
	bus.Publish(makeEnv(5))
	bus.Publish(makeEnv(9))

	require.Len(t, seqs, 3)
	// Sequences must be distinct and ordered.
	assert.Equal(t, seqs[0]+1, seqs[1])
	assert.Equal(t, seqs[1]+1, seqs[2])
}

func TestEventBus_Sequence_NotAssignedOnNilPublish(t *testing.T) {
	// A caller that constructs an envelope and never publishes it should
	// retain Sequence == 0 (the zero value).
	env := makeEnv(1)
	assert.Equal(t, uint64(0), env.Sequence)
}

func TestEventBus_Sequence_ConcurrentPublish_AllUnique(t *testing.T) {
	bus := runtime.NewEventBus()

	const workers = 8
	const perWorker = 50

	var mu sync.Mutex
	seen := make(map[uint64]struct{}, workers*perWorker)

	bus.Subscribe(func(e runtime.EventEnvelope) {
		mu.Lock()
		seen[e.Sequence] = struct{}{}
		mu.Unlock()
	})

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				bus.Publish(makeEnv(1))
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, workers*perWorker, len(seen), "every published event must have a unique Sequence")
}
