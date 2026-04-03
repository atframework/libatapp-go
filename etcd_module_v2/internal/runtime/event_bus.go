package runtime

import (
	"sync"
	"sync/atomic"
	"time"
)

// ── Event identity types ────────────────────────────────────────────────────

// EventType identifies the semantic of an EventEnvelope.
// The zero value (0) is reserved as EventTypeUnknown and acts as a wildcard
// in SubscribeType call matching — Subscribe() uses this implicitly.
type EventType uint32

// EventSource enumerates the actors that may produce events.
type EventSource uint8

const (
	EventSourceLeaseActor        EventSource = iota + 1
	EventSourceRegistrationActor             // 2
	EventSourceWatchActor                    // 3
	EventSourceProjectionActor               // 4
)

// Name returns a human-readable label for logging and metrics.
func (s EventSource) Name() string {
	switch s {
	case EventSourceLeaseActor:
		return "lease-actor"
	case EventSourceRegistrationActor:
		return "registration-actor"
	case EventSourceWatchActor:
		return "watch-actor"
	case EventSourceProjectionActor:
		return "projection-actor"
	default:
		return "unknown-actor"
	}
}

// ── EventEnvelope ──────────────────────────────────────────────────────────

// EventEnvelope is the single currency type exchanged on the EventBus.
// Fields are intentionally observable (LeaseEpoch, DedupeKey) to
// support idempotent projection and cross-actor ordering.
type EventEnvelope struct {
	Type EventType
	// Version is the envelope schema version; consumers can use it for forward
	// compatibility.  Current value: 1.
	Version    uint16
	Source     EventSource
	// Sequence is a bus-global monotonically increasing counter assigned by
	// EventBus.Publish.  It provides a total order across all events on this
	// bus instance and can be used for tracing and dedup without per-actor
	// counters.  Zero value means the envelope was never published through the bus.
	Sequence   uint64
	LeaseEpoch uint64 // incremented on every lease rebuild
	OccurredAt time.Time
	TraceID    string
	// DedupeKey is an idempotency key; consumers (e.g. ProjectionActor) may
	// discard duplicate envelopes with the same key.
	DedupeKey string
	// Payload carries the event-specific data.  Each EventType has a
	// corresponding concrete struct defined in orchestrator/events.go.
	Payload any
}

// ── EventBus ──────────────────────────────────────────────────────────────

// EventHandleHandle is the opaque handle returned by Subscribe* calls.
type EventHandleHandle int64

// EventHandler is the callback signature for bus subscribers.
type EventHandler func(envelope EventEnvelope)

// EventBus is the in-process, synchronous publish-subscribe bus shared by all
// actors within a single EtcdModule instance.
//
// Publish is called on the producer's goroutine; all subscribed handlers run
// synchronously in that same goroutine.  Handlers must be fast and
// non-blocking.
type EventBus interface {
	// Subscribe registers handler for ALL event types.
	Subscribe(handler EventHandler) EventHandleHandle
	// SubscribeType registers handler for a specific EventType only.
	SubscribeType(eventType EventType, handler EventHandler) EventHandleHandle
	// Unsubscribe removes a previously registered handler.
	Unsubscribe(handle EventHandleHandle)
	// Publish dispatches env to every matching registered handler.
	Publish(env EventEnvelope)
	// Close drains and disables the bus; subsequent Publish calls are no-ops.
	Close()
}

// ── defaultEventBus ───────────────────────────────────────────────────────

type handlerEntry struct {
	handler   EventHandler
	eventType EventType // zero value = wildcard (all types)
}

// defaultEventBus is the standard in-process EventBus implementation.
type defaultEventBus struct {
	mu        sync.RWMutex
	handlers  map[EventHandleHandle]handlerEntry
	nextID    atomic.Int64
	globalSeq atomic.Uint64
	closed    atomic.Bool
}

// NewEventBus creates a ready-to-use EventBus.
func NewEventBus() EventBus {
	return &defaultEventBus{
		handlers: make(map[EventHandleHandle]handlerEntry),
	}
}

func (b *defaultEventBus) Subscribe(handler EventHandler) EventHandleHandle {
	return b.subscribe(handlerEntry{handler: handler})
}

func (b *defaultEventBus) SubscribeType(eventType EventType, handler EventHandler) EventHandleHandle {
	return b.subscribe(handlerEntry{handler: handler, eventType: eventType})
}

func (b *defaultEventBus) subscribe(entry handlerEntry) EventHandleHandle {
	if entry.handler == nil || b.closed.Load() {
		return 0
	}
	id := EventHandleHandle(b.nextID.Add(1))
	b.mu.Lock()
	b.handlers[id] = entry
	b.mu.Unlock()
	return id
}

func (b *defaultEventBus) Unsubscribe(handle EventHandleHandle) {
	b.mu.Lock()
	delete(b.handlers, handle)
	b.mu.Unlock()
}

func (b *defaultEventBus) Publish(env EventEnvelope) {
	if b.closed.Load() {
		return
	}
	// Assign a bus-global monotonic sequence number before dispatching.
	env.Sequence = b.globalSeq.Add(1)
	// Snapshot the handler list under read-lock so handlers run lock-free.
	b.mu.RLock()
	snapshot := make([]EventHandler, 0, len(b.handlers))
	for _, entry := range b.handlers {
		if entry.eventType == 0 || entry.eventType == env.Type {
			snapshot = append(snapshot, entry.handler)
		}
	}
	b.mu.RUnlock()

	for _, h := range snapshot {
		h(env)
	}
}

func (b *defaultEventBus) Close() {
	b.closed.Store(true)
	b.mu.Lock()
	b.handlers = make(map[EventHandleHandle]handlerEntry)
	b.mu.Unlock()
}
