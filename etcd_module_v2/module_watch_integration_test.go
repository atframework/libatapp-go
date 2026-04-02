package modulev2_test

// Integration tests for watch-related EtcdModule facade APIs.
//
// Motivation: the unit tests in internal/orchestrator/watch_actor_test.go
// wire actors and buses directly (half-integration).  The tests below exercise
// the SAME behavioural invariants end-to-end through the public EtcdModule
// facade — NewEtcdModule → Start → AddWatchPrefix / RemoveWatchPrefix /
// ReloadWatchStreams → Subscribe — backed by a real in-process mockserver.
//
// Each test here corresponds 1-to-1 with a test in watch_actor_test.go:
//
//   WatchActor unit test                              │ Module integration test
//   ─────────────────────────────────────────────────┼────────────────────────────────────────────────
//   TestWatchActor_ByID_SnapshotFlow                  │ TestModule_Watch_ByID_SnapshotFlow
//   TestWatchActor_ByName_SnapshotFlow                │ TestModule_Watch_ByName_SnapshotFlow
//   TestWatchActor_Topology_SnapshotFlow              │ TestModule_Watch_Topology_SnapshotFlow
//   TestWatchActor_Topology_DoesNotEmitDiscoveryEvents│ TestModule_Watch_Topology_DoesNotEmitDiscoveryEvents
//   TestWatchActor_ThreePrefixes_EventsAreIsolated    │ TestModule_Watch_ThreePrefixes_EventsAreIsolated
//   TestWatchActor_AddPrefix_Idempotent               │ TestModule_AddWatchPrefix_Idempotent
//   TestWatchActor_SnapshotLoadedPayload_NodesInit… │ TestModule_Watch_GetSnapshot_InitiallyEmpty
//   TestWatchActor_RemovePrefix_AllowsReAdd           │ TestModule_RemoveWatchPrefix_AllowsReAdd
//   TestWatchActor_RemovePrefix_ExcludedFromActiveAll │ TestModule_RemoveWatchPrefix_ExcludedFromReloadStreams
//   TestWatchActor_ActiveAll_RestartsExistingStream   │ TestModule_ReloadWatchStreams_RestartsPrefixes
//   TestWatchActor_ActiveAll_NoOp_WhenNoPrefixes      │ TestModule_ReloadWatchStreams_NoOp_WhenNoPrefixes

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mock/mockserver"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
)

// ── Fixture key prefixes ──────────────────────────────────────────────────

const (
	watchIntegByIDPrefix   = "/svc/by_id"
	watchIntegByNamePrefix = "/svc/by_name"
	watchIntegTopoPrefix   = "/svc/topology"
)

// ── Helpers ───────────────────────────────────────────────────────────────

// startWatchIntegModule creates and starts an EtcdModule backed by a single-node
// in-process mockserver.  watchPrefixes is registered as WatchPrefixes in the
// PathConfig (pass nil for no initial prefixes).
// Stop() is called via t.Cleanup; the EtcdModule owns the *clientv3.Client.
func startWatchIntegModule(t *testing.T, watchPrefixes []string) *modulev2.EtcdModule {
	t.Helper()

	servers, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	t.Cleanup(servers.Stop)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{servers.Servers[0].Address},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	// EtcdModule.Stop() closes the client — do NOT register cli.Close() here.

	cfg := modulev2.PathConfig{
		ByIDPrefix:     watchIntegByIDPrefix,
		ByNamePrefix:   watchIntegByNamePrefix,
		TopologyPrefix: watchIntegTopoPrefix,
		WatchPrefixes:  watchPrefixes,
		LeaseTTL:       30,
	}
	m := modulev2.NewEtcdModule(cli, cfg, modulev2.ModuleOptions{
		RetryInterval: 100 * time.Millisecond,
	})
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Stop(ctx)
	})
	return m
}

// subscribeWatchIntegEvents subscribes to all events on m and returns a
// buffered channel.  The subscription handle is removed via t.Cleanup.
func subscribeWatchIntegEvents(t *testing.T, m *modulev2.EtcdModule) <-chan modulev2.EventEnvelope {
	t.Helper()
	ch := make(chan modulev2.EventEnvelope, 256)
	handle := m.Subscribe(func(e modulev2.EventEnvelope) {
		select {
		case ch <- e:
		default:
		}
	})
	t.Cleanup(func() { m.Unsubscribe(handle) })
	return ch
}

// drainWatchIntegEvents collects every event that arrives within timeout.
func drainWatchIntegEvents(ch <-chan modulev2.EventEnvelope, timeout time.Duration) []modulev2.EventEnvelope {
	var out []modulev2.EventEnvelope
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

// waitForWatchIntegEvent blocks until an event of wantType arrives or times out.
func waitForWatchIntegEvent(
	t *testing.T,
	ch <-chan modulev2.EventEnvelope,
	wantType modulev2.EventType,
	timeout time.Duration,
) modulev2.EventEnvelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env := <-ch:
			if env.Type == wantType {
				return env
			}
		case <-deadline:
			t.Fatalf("timed out waiting for module event %s", modulev2.EventTypeName(wantType))
			return modulev2.EventEnvelope{}
		}
	}
}

// ── Tests: snapshot flow ──────────────────────────────────────────────────

// TestModule_Watch_ByID_SnapshotFlow corresponds to TestWatchActor_ByID_SnapshotFlow.
// It verifies that EtcdModule.AddWatchPrefix("/svc/by_id") causes the module's
// event bus to publish EventWatchSnapshotLoading then EventWatchSnapshotLoaded,
// both carrying the correct Prefix, observable via module.Subscribe.
func TestModule_Watch_ByID_SnapshotFlow(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))

	loadingEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(modulev2.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByIDPrefix, pl.Prefix)

	loadedEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByIDPrefix, lpl.Prefix)
}

// TestModule_Watch_ByName_SnapshotFlow corresponds to TestWatchActor_ByName_SnapshotFlow.
func TestModule_Watch_ByName_SnapshotFlow(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByNamePrefix))

	loadingEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(modulev2.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByNamePrefix, pl.Prefix)

	loadedEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByNamePrefix, lpl.Prefix)
}

// TestModule_Watch_Topology_SnapshotFlow corresponds to TestWatchActor_Topology_SnapshotFlow.
// It verifies that the topology prefix generates the topology-specific event pair
// (EventWatchTopologySnapshotLoading / EventWatchTopologySnapshotLoaded) through
// the module façade.
func TestModule_Watch_Topology_SnapshotFlow(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegTopoPrefix))

	waitForWatchIntegEvent(t, ch, modulev2.EventWatchTopologySnapshotLoading, 5*time.Second)

	loadedEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 5*time.Second)
	_, ok := loadedEnv.Payload.(modulev2.WatchTopologySnapshotLoadedPayload)
	require.True(t, ok, "payload must be WatchTopologySnapshotLoadedPayload")
}

// TestModule_Watch_Topology_DoesNotEmitDiscoveryEvents corresponds to
// TestWatchActor_Topology_DoesNotEmitDiscoveryEvents.
// It verifies that adding only the topology prefix does not produce discovery
// snapshot events.
func TestModule_Watch_Topology_DoesNotEmitDiscoveryEvents(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegTopoPrefix))
	waitForWatchIntegEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 5*time.Second)

	remaining := drainWatchIntegEvents(ch, 1500*time.Millisecond)
	for _, e := range remaining {
		assert.NotEqual(t, modulev2.EventWatchSnapshotLoading, e.Type,
			"topology prefix must not emit discovery Loading event")
		assert.NotEqual(t, modulev2.EventWatchSnapshotLoaded, e.Type,
			"topology prefix must not emit discovery Loaded event")
	}
}

// TestModule_Watch_ThreePrefixes_EventsAreIsolated corresponds to
// TestWatchActor_ThreePrefixes_EventsAreIsolated.
// It verifies that registering all three prefixes through the facade produces
// independent snapshot cycles and correct prefix tags.
func TestModule_Watch_ThreePrefixes_EventsAreIsolated(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))
	require.NoError(t, m.AddWatchPrefix(watchIntegByNamePrefix))
	require.NoError(t, m.AddWatchPrefix(watchIntegTopoPrefix))

	collected := drainWatchIntegEvents(ch, 3*time.Second)

	byType := make(map[modulev2.EventType]int)
	for _, e := range collected {
		byType[e.Type]++
	}

	assert.GreaterOrEqual(t, byType[modulev2.EventWatchSnapshotLoading], 2,
		"two discovery prefixes → at least two Loading events")
	assert.GreaterOrEqual(t, byType[modulev2.EventWatchSnapshotLoaded], 2,
		"two discovery prefixes → at least two Loaded events")
	assert.GreaterOrEqual(t, byType[modulev2.EventWatchTopologySnapshotLoading], 1)
	assert.GreaterOrEqual(t, byType[modulev2.EventWatchTopologySnapshotLoaded], 1)

	for _, e := range collected {
		if e.Type == modulev2.EventWatchSnapshotLoaded {
			lpl := e.Payload.(modulev2.WatchSnapshotLoadedPayload)
			assert.True(t,
				lpl.Prefix == watchIntegByIDPrefix || lpl.Prefix == watchIntegByNamePrefix,
				"unexpected SnapshotLoaded prefix: %q", lpl.Prefix)
		}
	}
}

// ── Tests: AddWatchPrefix ─────────────────────────────────────────────────

// TestModule_AddWatchPrefix_Idempotent corresponds to TestWatchActor_AddPrefix_Idempotent.
// It verifies that calling AddWatchPrefix twice with the same prefix through the
// facade starts only one watch stream, producing exactly one Loading and one
// Loaded event for that prefix.
func TestModule_AddWatchPrefix_Idempotent(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))
	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix)) // duplicate — must be a no-op

	collected := drainWatchIntegEvents(ch, 2*time.Second)

	var loadingCount, loadedCount int
	for _, e := range collected {
		switch e.Type {
		case modulev2.EventWatchSnapshotLoading:
			if e.Payload.(modulev2.WatchSnapshotLoadingPayload).Prefix == watchIntegByIDPrefix {
				loadingCount++
			}
		case modulev2.EventWatchSnapshotLoaded:
			if e.Payload.(modulev2.WatchSnapshotLoadedPayload).Prefix == watchIntegByIDPrefix {
				loadedCount++
			}
		}
	}

	assert.Equal(t, 1, loadingCount, "duplicate AddWatchPrefix must not start a second stream")
	assert.Equal(t, 1, loadedCount, "duplicate AddWatchPrefix must not produce a second Loaded event")
}

// ── Tests: snapshot content via GetSnapshot ───────────────────────────────

// TestModule_Watch_GetSnapshot_InitiallyEmpty corresponds to
// TestWatchActor_SnapshotLoadedPayload_NodesInitiallyEmpty.
// It verifies that after the initial snapshot load, GetSnapshot() returns a
// non-nil snapshot with an empty discovery node set (mockserver has no keys).
func TestModule_Watch_GetSnapshot_InitiallyEmpty(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))
	waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)

	// Give ProjectionActor time to apply the event and publish the snapshot.
	require.Eventually(t, func() bool {
		return m.GetSnapshot() != nil
	}, 3*time.Second, 10*time.Millisecond, "GetSnapshot must be non-nil after initial load")

	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	assert.Empty(t, snap.Discovery.NodesByPath,
		"no keys in mockserver → discovery NodesByPath must be empty")
	assert.NotNil(t, snap, "snapshot itself must not be nil")
}

// ── Tests: RemoveWatchPrefix ──────────────────────────────────────────────

// TestModule_RemoveWatchPrefix_AllowsReAdd corresponds to
// TestWatchActor_RemovePrefix_AllowsReAdd.
// It verifies that after RemoveWatchPrefix the actor no longer tracks the
// prefix, so a subsequent AddWatchPrefix via the facade restarts a fresh stream.
func TestModule_RemoveWatchPrefix_AllowsReAdd(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	// First stream.
	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))
	waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)

	require.NoError(t, m.RemoveWatchPrefix(watchIntegByIDPrefix))
	drainWatchIntegEvents(ch, 300*time.Millisecond)

	// Re-add must open a brand-new stream.
	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))

	loadingEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(modulev2.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByIDPrefix, pl.Prefix)

	waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)
}

// TestModule_RemoveWatchPrefix_ExcludedFromReloadStreams corresponds to
// TestWatchActor_RemovePrefix_ExcludedFromActiveAll.
// It verifies that a prefix removed via RemoveWatchPrefix is not restarted
// when ReloadWatchStreams is called.
func TestModule_RemoveWatchPrefix_ExcludedFromReloadStreams(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))
	waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)

	require.NoError(t, m.RemoveWatchPrefix(watchIntegByIDPrefix))
	drainWatchIntegEvents(ch, 300*time.Millisecond)

	// ReloadWatchStreams on an empty set must produce no watch events.
	require.NoError(t, m.ReloadWatchStreams())
	remaining := drainWatchIntegEvents(ch, 600*time.Millisecond)

	for _, e := range remaining {
		assert.NotEqual(t, modulev2.EventWatchSnapshotLoading, e.Type,
			"removed prefix must not be restarted by ReloadWatchStreams")
		assert.NotEqual(t, modulev2.EventWatchSnapshotLoaded, e.Type,
			"removed prefix must not be restarted by ReloadWatchStreams")
	}
}

// ── Tests: ReloadWatchStreams ─────────────────────────────────────────────

// TestModule_ReloadWatchStreams_RestartsPrefixes corresponds to
// TestWatchActor_ActiveAll_RestartsExistingStream.
// It verifies that ReloadWatchStreams() cancels the current stream for all
// registered prefixes and opens fresh ones, producing a new Loading+Loaded pair
// observable via module.Subscribe.
func TestModule_ReloadWatchStreams_RestartsPrefixes(t *testing.T) {
	m := startWatchIntegModule(t, nil)
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(watchIntegByIDPrefix))
	waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)
	drainWatchIntegEvents(ch, 300*time.Millisecond)

	// Force-restart all streams.
	require.NoError(t, m.ReloadWatchStreams())

	loadingEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoading, 5*time.Second)
	pl, ok := loadingEnv.Payload.(modulev2.WatchSnapshotLoadingPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByIDPrefix, pl.Prefix)

	loadedEnv := waitForWatchIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchSnapshotLoadedPayload)
	require.True(t, ok)
	assert.Equal(t, watchIntegByIDPrefix, lpl.Prefix)
}

// TestModule_ReloadWatchStreams_NoOp_WhenNoPrefixes corresponds to
// TestWatchActor_ActiveAll_NoOp_WhenNoPrefixes.
// It verifies that ReloadWatchStreams() on a module with no registered prefixes
// does not emit any watch events.
func TestModule_ReloadWatchStreams_NoOp_WhenNoPrefixes(t *testing.T) {
	m := startWatchIntegModule(t, nil) // no watch prefixes
	ch := subscribeWatchIntegEvents(t, m)

	require.NoError(t, m.ReloadWatchStreams())

	events := drainWatchIntegEvents(ch, 500*time.Millisecond)
	for _, e := range events {
		assert.NotEqual(t, modulev2.EventWatchSnapshotLoading, e.Type,
			"no prefixes → ReloadWatchStreams must not emit Loading events")
		assert.NotEqual(t, modulev2.EventWatchSnapshotLoaded, e.Type,
			"no prefixes → ReloadWatchStreams must not emit Loaded events")
		assert.NotEqual(t, modulev2.EventWatchTopologySnapshotLoading, e.Type,
			"no prefixes → ReloadWatchStreams must not emit topology Loading events")
		assert.NotEqual(t, modulev2.EventWatchTopologySnapshotLoaded, e.Type,
			"no prefixes → ReloadWatchStreams must not emit topology Loaded events")
	}
}
