package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// ── helpers ───────────────────────────────────────────────────────────────

// drainEvents collects all events that arrive within timeout into a slice.
func drainEvents(ch <-chan runtime.EventEnvelope, timeout time.Duration) []runtime.EventEnvelope {
	var out []runtime.EventEnvelope
	deadline := time.After(timeout)
	for {
		select {
		case env := <-ch:
			out = append(out, env)
		case <-deadline:
			return out
		}
	}
}

// waitForEventType blocks until an event of wantType arrives or times out.
func waitForEventType(t *testing.T, ch <-chan runtime.EventEnvelope, wantType runtime.EventType, timeout time.Duration) runtime.EventEnvelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env := <-ch:
			if env.Type == wantType {
				return env
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event type %v", wantType)
			return runtime.EventEnvelope{}
		}
	}
}

// startWatchActor starts a WatchActor backed by a real etcd client and returns
// a channel that receives all events published to the shared bus.
func startWatchActor(t *testing.T, cli *clientv3.Client) (*orchestrator.WatchActor, <-chan runtime.EventEnvelope, context.CancelFunc) {
	t.Helper()
	bus := runtime.NewEventBus()

	bufCh := make(chan runtime.EventEnvelope, 128)
	bus.Subscribe(func(e runtime.EventEnvelope) {
		select {
		case bufCh <- e:
		default:
		}
	})

	actor := orchestrator.NewWatchActor(cli, bus)
	ctx, cancel := context.WithCancel(context.Background())
	go actor.Run(ctx)
	return actor, bufCh, cancel
}

// ── Path 1: /by_id — discovery stream ────────────────────────────────────

// TestWatchActor_ByID_SnapshotFlow verifies that after WatchActor.AddPrefix("/svc/by_id")
// the actor publishes:
//  1. EventWatchSnapshotLoading — carrying the "/svc/by_id" prefix.
//  2. EventWatchSnapshotLoaded  — also carrying the correct prefix.
//
// Note: mock/mockserver does not persist Put values, so the initial Get returns
// zero nodes. This test only validates the event-flow and prefix metadata.
func TestWatchActor_ByID_SnapshotFlow(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_id")

	// Step 1: Loading event must carry the correct prefix.
	loadingEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(orchestrator.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_id", pl.Prefix)

	// Step 2: Loaded event must also carry the correct prefix.
	loadedEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(orchestrator.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_id", lpl.Prefix)
}

// ── Path 2: /by_name — discovery stream ──────────────────────────────────

// TestWatchActor_ByName_SnapshotFlow mirrors the /by_id snapshot flow test on
// the /by_name prefix, confirming the prefix tag is correctly propagated for
// name-indexed discovery streams.
func TestWatchActor_ByName_SnapshotFlow(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_name")

	loadingEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(orchestrator.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_name", pl.Prefix)

	loadedEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(orchestrator.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_name", lpl.Prefix)
}

// ── Path 3: /topology — topology stream ──────────────────────────────────

// TestWatchActor_Topology_SnapshotFlow verifies that the topology prefix follows
// the topology-specific event path (EventWatchTopologySnapshotLoading /
// EventWatchTopologySnapshotLoaded) and does NOT emit discovery events.
func TestWatchActor_Topology_SnapshotFlow(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/topology")

	// LoadingEvent for topology does not carry a Prefix field (different payload type).
	waitForEventType(t, evCh, runtime.EventWatchTopologySnapshotLoading, 5*time.Second)

	loadedEnv := waitForEventType(t, evCh, runtime.EventWatchTopologySnapshotLoaded, 5*time.Second)
	_, ok := loadedEnv.Payload.(orchestrator.WatchTopologySnapshotLoadedPayload)
	require.True(t, ok, "topology Loaded payload must be WatchTopologySnapshotLoadedPayload")
}

// TestWatchActor_Topology_DoesNotEmitDiscoveryEvents verifies that adding a
// topology prefix does not produce any discovery-path events (EventWatchSnapshotLoading /
// EventWatchSnapshotLoaded), confirming the routing predicate is correct.
func TestWatchActor_Topology_DoesNotEmitDiscoveryEvents(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/topology")

	// Drain all events for 1.5 s after the topology snapshot completes.
	waitForEventType(t, evCh, runtime.EventWatchTopologySnapshotLoaded, 5*time.Second)
	remaining := drainEvents(evCh, 1500*time.Millisecond)

	for _, e := range remaining {
		assert.NotEqual(t, runtime.EventWatchSnapshotLoading, e.Type,
			"topology prefix must not emit discovery Loading event")
		assert.NotEqual(t, runtime.EventWatchSnapshotLoaded, e.Type,
			"topology prefix must not emit discovery Loaded event")
	}
}

// ── Cross-prefix isolation ────────────────────────────────────────────────

// TestWatchActor_ThreePrefixes_EventsAreIsolated verifies that when all three
// prefixes are registered simultaneously, each emits its own independent
// Loading+Loaded pair and the prefix tags in the discovery Loaded events are correct.
func TestWatchActor_ThreePrefixes_EventsAreIsolated(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_id")
	actor.AddPrefix("/svc/by_name")
	actor.AddPrefix("/svc/topology")

	// Collect all events that arrive within 2 s.
	collected := drainEvents(evCh, 2*time.Second)

	byType := make(map[runtime.EventType]int)
	for _, e := range collected {
		byType[e.Type]++
	}

	// Each discovery prefix emits one Loading + one Loaded.
	assert.GreaterOrEqual(t, byType[runtime.EventWatchSnapshotLoading], 2,
		"two discovery prefixes → at least two Loading events")
	assert.GreaterOrEqual(t, byType[runtime.EventWatchSnapshotLoaded], 2,
		"two discovery prefixes → at least two Loaded events")

	// Topology prefix emits its own Loading + Loaded pair.
	assert.GreaterOrEqual(t, byType[runtime.EventWatchTopologySnapshotLoading], 1)
	assert.GreaterOrEqual(t, byType[runtime.EventWatchTopologySnapshotLoaded], 1)

	// Every discovery SnapshotLoaded Prefix must belong to /by_id or /by_name.
	for _, e := range collected {
		if e.Type == runtime.EventWatchSnapshotLoaded {
			lpl := e.Payload.(orchestrator.WatchSnapshotLoadedPayload)
			assert.True(t,
				lpl.Prefix == "/svc/by_id" || lpl.Prefix == "/svc/by_name",
				"unexpected SnapshotLoaded prefix: %q", lpl.Prefix)
		}
	}
}

// ── AddPrefix — idempotency ───────────────────────────────────────────────

// TestWatchActor_AddPrefix_Idempotent verifies that calling AddPrefix twice
// with the same prefix starts only ONE watch stream, producing exactly one
// Loading and one Loaded event for that prefix.
func TestWatchActor_AddPrefix_Idempotent(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_id")
	actor.AddPrefix("/svc/by_id") // duplicate — must be a no-op

	collected := drainEvents(evCh, 2*time.Second)

	var loadingCount, loadedCount int
	for _, e := range collected {
		switch e.Type {
		case runtime.EventWatchSnapshotLoading:
			pl := e.Payload.(orchestrator.WatchSnapshotLoadingPayload)
			if pl.Prefix == "/svc/by_id" {
				loadingCount++
			}
		case runtime.EventWatchSnapshotLoaded:
			pl := e.Payload.(orchestrator.WatchSnapshotLoadedPayload)
			if pl.Prefix == "/svc/by_id" {
				loadedCount++
			}
		}
	}

	assert.Equal(t, 1, loadingCount, "duplicate AddPrefix must not start a second stream")
	assert.Equal(t, 1, loadedCount, "duplicate AddPrefix must not produce a second Loaded event")
}

// TestWatchActor_SnapshotLoadedPayload_NodesInitiallyEmpty verifies that the
// Nodes map in WatchSnapshotLoadedPayload is non-nil but empty when mockserver
// has no keys stored for the prefix (no Put was issued).
func TestWatchActor_SnapshotLoadedPayload_NodesInitiallyEmpty(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_id")

	loadedEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(orchestrator.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.NotNil(t, lpl.Nodes, "Nodes map must be non-nil even when empty")
	assert.Empty(t, lpl.Nodes, "no keys in mockserver → empty snapshot")
}

// ── RemovePrefix ──────────────────────────────────────────────────────────

// TestWatchActor_RemovePrefix_AllowsReAdd verifies that after RemovePrefix the
// actor no longer tracks the prefix, so a subsequent AddPrefix starts a fresh
// stream (produces a new Loading+Loaded pair).  If RemovePrefix were a no-op,
// the second AddPrefix would be treated as a duplicate and would be silently
// dropped — no new events would arrive.
func TestWatchActor_RemovePrefix_AllowsReAdd(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	// First stream.
	actor.AddPrefix("/svc/by_id")
	waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)

	// Remove and drain any residual events (e.g. stream-close artefacts).
	actor.RemovePrefix("/svc/by_id")
	drainEvents(evCh, 300*time.Millisecond)

	// Re-add — must produce a brand-new Loading event.
	actor.AddPrefix("/svc/by_id")
	env := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := env.Payload.(orchestrator.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_id", pl.Prefix)

	// And the corresponding Loaded.
	waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)
}

// TestWatchActor_RemovePrefix_ExcludedFromActiveAll verifies that a prefix
// removed via RemovePrefix is not re-opened when ActiveAll is called.
// With only prefix A registered, removing A then calling ActiveAll should
// produce no new events.
func TestWatchActor_RemovePrefix_ExcludedFromActiveAll(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_id")
	waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)

	actor.RemovePrefix("/svc/by_id")
	drainEvents(evCh, 300*time.Millisecond)

	// ActiveAll on an empty set must produce no stream events.
	actor.ActiveAll()
	remaining := drainEvents(evCh, 600*time.Millisecond)

	for _, e := range remaining {
		assert.NotEqual(t, runtime.EventWatchSnapshotLoading, e.Type,
			"removed prefix must not be restarted by ActiveAll")
		assert.NotEqual(t, runtime.EventWatchSnapshotLoaded, e.Type,
			"removed prefix must not be restarted by ActiveAll")
	}
}

// ── ActiveAll ─────────────────────────────────────────────────────────────

// TestWatchActor_ActiveAll_RestartsExistingStream verifies that ActiveAll
// cancels the current stream for all registered prefixes and opens fresh ones,
// producing a new Loading+Loaded pair each time.
func TestWatchActor_ActiveAll_RestartsExistingStream(t *testing.T) {
	cli := startMockEtcd(t)
	actor, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	actor.AddPrefix("/svc/by_id")
	// Wait for the first full snapshot cycle.
	waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)
	drainEvents(evCh, 300*time.Millisecond)

	// Force-restart all streams.
	actor.ActiveAll()

	// Must see a fresh Loading for the same prefix.
	loadingEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(orchestrator.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_id", pl.Prefix)

	// And its Loaded counterpart.
	loadedEnv := waitForEventType(t, evCh, runtime.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(orchestrator.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.Equal(t, "/svc/by_id", lpl.Prefix)
}

// TestWatchActor_ActiveAll_NoOp_WhenNoPrefixes verifies that ActiveAll on an
// actor with no registered prefixes does not emit any events.
func TestWatchActor_ActiveAll_NoOp_WhenNoPrefixes(t *testing.T) {
	cli := startMockEtcd(t)
	_, evCh, cancel := startWatchActor(t, cli)
	defer cancel()

	// No AddPrefix; call ActiveAll immediately.
	// (actor is started inside startWatchActor, so we can call API right away)
	// We have no way to call ActiveAll without the actor reference, so re-build:
	actor2, evCh2, cancel2 := startWatchActor(t, cli)
	defer cancel2()
	actor2.ActiveAll()
	events := drainEvents(evCh2, 500*time.Millisecond)
	_ = evCh // silence unused warning
	assert.Empty(t, events, "ActiveAll with no prefixes must emit no events")
}
