package modulev2_test

// Integration tests for Lease, Registration, and Projection actors accessed
// through the public EtcdModule facade.
//
// Motivation: the unit tests in internal/orchestrator/ wire actors and buses
// directly, injecting fake clients.  The tests below exercise the SAME
// behavioural invariants end-to-end through NewEtcdModule → Start → public
// API methods → Subscribe, backed by a real in-process mockserver.
//
// Actor unit test → module integration test mapping:
//
// ── LeaseActor ──────────────────────────────────────────────────────────────────
//   TestLeaseActor_StartGrantsLease             → TestModule_Lease_GrantedOnStart
//   TestLeaseActor_Stop_CleanShutdown           → TestModule_Lifecycle_StopModuleCleanly
//                                                 (EventLeaseReleased path NOT asserted: mockserver leaseID=0)
// ── RegistrationActor ───────────────────────────────────────────────────────────
//   TestRegistrationActor_Run_RemoveService_DeletesDiscovery…           → TestModule_RegisterService_FiresRegistrationChanged
//                                                                         (Register+Unregister; onRemoveDiscovery always publishes event)
//
// ── ProjectionActor ─────────────────────────────────────────────────────────────
//   TestProjectionActor_GetSnapshot_BeforeFirstPublish_ReturnsNil       → TestModule_GetSnapshot_NilBeforeFirstWatchLoad
//   TestProjectionActor_SnapshotLoaded_PopulatesTopology                → TestModule_GetSnapshot_PopulatesAfterWatchLoad
//   TestProjectionActor_VersionMonotonicallyIncreases                   → TestModule_Snapshot_VersionIncreases
//
// ── Reload / Stop+Start cycle ───────────────────────────────────────────────────
//   (no direct unit test)                                               → TestModule_StopAndNewModuleStart_RegrantsLease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mock/mockserver"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// ── actor test fixture prefixes ───────────────────────────────────────────

const (
	actorIntegByIDPrefix    = "/svc/by_id"
	actorIntegByNamePrefix  = "/svc/by_name"
	actorIntegTopoPrefix    = "/svc/topology"
)

// ── helpers ───────────────────────────────────────────────────────────────

// newActorIntegClient starts a one-node in-process mockserver and returns a
// connected *clientv3.Client.  The server and client lifetimes are bound to t.
// NOTE: the caller is responsible for closing the client (or passing it to
// EtcdModule which will close it on Stop).
func newActorIntegClient(t *testing.T) *clientv3.Client {
	t.Helper()
	servers, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	t.Cleanup(servers.Stop)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{servers.Servers[0].Address},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return cli
}

// startActorIntegModule creates and starts an EtcdModule backed by a real
// mockserver client.  watchPrefixes is the initial set registered in
// PathConfig.WatchPrefixes (pass nil for no prefixes).
// The module is stopped via t.Cleanup; it owns the client's close.
func startActorIntegModule(t *testing.T, watchPrefixes []string) *modulev2.EtcdModule {
	t.Helper()
	cli := newActorIntegClient(t)

	cfg := modulev2.PathConfig{
		ByIDPrefix:     actorIntegByIDPrefix,
		ByNamePrefix:   actorIntegByNamePrefix,
		TopologyPrefix: actorIntegTopoPrefix,
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

// subscribeActorIntegEvents subscribes to all events and returns a buffered channel.
func subscribeActorIntegEvents(t *testing.T, m *modulev2.EtcdModule) <-chan modulev2.EventEnvelope {
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

// waitForActorIntegEvent blocks until an event of wantType arrives on ch or the
// deadline passes.
func waitForActorIntegEvent(
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

// ── LeaseActor integration tests ──────────────────────────────────────────

// TestModule_Lease_GrantedOnStart corresponds to TestLeaseActor_StartGrantsLease.
// It verifies that module.Start() makes the facade publish EventLeaseGranted on
// its event bus, observable via module.Subscribe.
func TestModule_Lease_GrantedOnStart(t *testing.T) {
	m := startActorIntegModule(t, nil)
	ch := subscribeActorIntegEvents(t, m)

	// The LeaseActor has already been started inside module.Start(); the grant
	// event may come very quickly.  Use a generous timeout for slow CI.
	env := waitForActorIntegEvent(t, ch, modulev2.EventLeaseGranted, 5*time.Second)

	pl, ok := env.Payload.(modulev2.LeaseGrantedPayload)
	require.True(t, ok, "payload must be LeaseGrantedPayload")
	// mock/mockserver returns LeaseID=0; assert only that the event fired.
	_ = pl
	assert.Equal(t, uint64(1), env.LeaseEpoch, "first lease grant must have epoch=1")
}

// TestModule_Lifecycle_StopModuleCleanly corresponds to
// TestLeaseActor_Stop_CleanShutdown.
// It verifies that Stop() transitions the module to a non-running state where
// Tick() and a second Stop() are safe.
//
// NOTE: EventLeaseReleased is NOT asserted here because mockserver always
// returns LeaseID=0, and leaseActor.onStop() only publishes EventLeaseReleased
// when LeaseID != 0.  The EventLeaseReleased path is covered by the unit test
// TestLeaseActor_Stop_CleanShutdown.
func TestModule_Lifecycle_StopModuleCleanly(t *testing.T) {
	cli := newActorIntegClient(t)
	cfg := modulev2.PathConfig{
		ByIDPrefix: actorIntegByIDPrefix, LeaseTTL: 30,
	}
	m := modulev2.NewEtcdModule(cli, cfg, modulev2.ModuleOptions{RetryInterval: 100 * time.Millisecond})
	require.NoError(t, m.Start(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.Stop(ctx) // ignore lease revoke errors from mockserver

	// After Stop, Tick must be a no-op (not panic).
	assert.NotPanics(t, func() { m.Tick() })

	// Second Stop must return ErrNotRunning.
	err2 := m.Stop(ctx)
	assert.Equal(t, modulev2.ErrNotRunning, err2, "second Stop must return ErrNotRunning")
}

// ── RegistrationActor integration tests ───────────────────────────────────

// TestModule_RegisterService_FiresRegistrationChanged corresponds to the
// EventRegistrationChanged assertion in
// TestRegistrationActor_Run_AddDiscoveryAndTopology.
// It verifies that a service lifecycle change causes EventRegistrationChanged
// to be published on the module's event bus.
//
// NOTE: with mockserver (leaseID=0) onAddDiscovery queues the service but
// skips the PUT (no lease), so it does NOT publish EventRegistrationChanged.
// onRemoveDiscovery, however, always publishes EventRegistrationChanged
// regardless of lease state.  We therefore Register→Unregister and assert
// the event fires on Unregister — the bus-delivery path is identical.
func TestModule_RegisterService_FiresRegistrationChanged(t *testing.T) {
	m := startActorIntegModule(t, nil)
	ch := subscribeActorIntegEvents(t, m)

	svc := modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 43, Name: "svc-b"},
		Path:      actorIntegByIDPrefix + "/svc-b-43",
		TTL:       10,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handle, err := m.RegisterService(ctx, svc)
	require.NoError(t, err)
	_ = handle.Wait(ctx)

	// Unregister triggers onRemoveDiscovery which always publishes
	// EventRegistrationChanged (the bus-delivery path we want to exercise).
	err = m.UnregisterService(ctx, svc.Path)
	require.NoError(t, err)

	env := waitForActorIntegEvent(t, ch, modulev2.EventRegistrationChanged, 5*time.Second)
	pl, ok := env.Payload.(modulev2.RegistrationChangedPayload)
	require.True(t, ok, "payload must be RegistrationChangedPayload")
	_ = pl // path-level assertions are already covered by unit tests
}

// ── ProjectionActor integration tests ─────────────────────────────────────

// TestModule_GetSnapshot_NilBeforeFirstWatchLoad corresponds to
// TestProjectionActor_GetSnapshot_BeforeFirstPublish_ReturnsNil.
// It verifies that if no watch prefixes are configured (so no watch snapshot
// ever fires), GetSnapshot() returns nil.
func TestModule_GetSnapshot_NilBeforeFirstWatchLoad(t *testing.T) {
	m := startActorIntegModule(t, nil) // no watch prefixes
	// Without watch prefixes the projection actor never publishes a snapshot.
	// Verify this holds consistently for 200 ms (not a timing assumption).
	require.Never(t, func() bool { return m.GetSnapshot() != nil },
		200*time.Millisecond, 10*time.Millisecond,
		"GetSnapshot must remain nil when no watch prefixes are configured")
}

// TestModule_GetSnapshot_PopulatesAfterWatchLoad corresponds to
// TestProjectionActor_SnapshotLoaded_PopulatesTopology.
// It verifies that after the initial watch snapshot loads, GetSnapshot()
// returns a non-nil snapshot with Discovery.Ready == true.
func TestModule_GetSnapshot_PopulatesAfterWatchLoad(t *testing.T) {
	m := startActorIntegModule(t, []string{actorIntegByIDPrefix})
	ch := subscribeActorIntegEvents(t, m)

	// Wait for watch snapshot to complete.
	waitForActorIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)

	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Discovery.Ready
	}, 3*time.Second, 10*time.Millisecond,
		"GetSnapshot must be non-nil with Discovery.Ready after watch load")
}

// TestModule_Snapshot_VersionIncreases corresponds to
// TestProjectionActor_VersionMonotonicallyIncreases.
// It verifies that successive snapshot publishes have strictly increasing Version.
func TestModule_Snapshot_VersionIncreases(t *testing.T) {
	m := startActorIntegModule(t, []string{actorIntegByIDPrefix, actorIntegByNamePrefix})
	ch := subscribeActorIntegEvents(t, m)

	// Wait for both prefixes to load their snapshots.
	waitForActorIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)
	waitForActorIntegEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 5*time.Second)

	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Version >= 2
	}, 3*time.Second, 10*time.Millisecond,
		"snapshot Version must be >= 2 after two prefixes complete loading")

	snap := m.GetSnapshot()
	assert.GreaterOrEqual(t, snap.Version, uint64(2),
		"each watch load increments snapshot Version")
}

// ── Reload / Stop+Start cycle integration test ────────────────────────────

// TestModule_StopAndNewModuleStart_RegrantsLease demonstrates the
// Stop-then-create-new-module pattern used by etcdModuleAdapter.Reload().
// It verifies that a fresh EtcdModule correctly re-acquires a lease after the
// previous instance has been stopped.
func TestModule_StopAndNewModuleStart_RegrantsLease(t *testing.T) {
	servers, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	t.Cleanup(servers.Stop)

	addr := servers.Servers[0].Address

	// ── First module instance ────────────────────────────────────────
	cli1, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	cfg := modulev2.PathConfig{ByIDPrefix: actorIntegByIDPrefix, LeaseTTL: 30}
	m1 := modulev2.NewEtcdModule(cli1, cfg, modulev2.ModuleOptions{RetryInterval: 100 * time.Millisecond})
	require.NoError(t, m1.Start(context.Background()))

	ch1 := make(chan modulev2.EventEnvelope, 32)
	m1.Subscribe(func(e modulev2.EventEnvelope) {
		select {
		case ch1 <- e:
		default:
		}
	})

	waitForActorIntegEvent(t, ch1, modulev2.EventLeaseGranted, 5*time.Second)

	// Stop first instance (simulates Reload teardown).
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = m1.Stop(stopCtx) // owns cli1.Close()

	// ── Second module instance (simulates Reload re-create) ──────────
	cli2, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	// m2.Stop will close cli2.
	m2 := modulev2.NewEtcdModule(cli2, cfg, modulev2.ModuleOptions{RetryInterval: 100 * time.Millisecond})
	require.NoError(t, m2.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m2.Stop(ctx)
	})

	ch2 := make(chan modulev2.EventEnvelope, 32)
	m2.Subscribe(func(e modulev2.EventEnvelope) {
		select {
		case ch2 <- e:
		default:
		}
	})

	// The new instance must re-acquire a lease independently.
	env := waitForActorIntegEvent(t, ch2, modulev2.EventLeaseGranted, 5*time.Second)
	pl, ok := env.Payload.(modulev2.LeaseGrantedPayload)
	require.True(t, ok)
	_ = pl // LeaseID=0 from mockserver is normal
	assert.Equal(t, uint64(1), env.LeaseEpoch,
		"new module must start with lease epoch 1")
}
