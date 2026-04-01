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
