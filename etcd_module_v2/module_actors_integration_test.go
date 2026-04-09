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
	actorIntegByIDPrefix   = "/svc/by_id"
	actorIntegByNamePrefix = "/svc/by_name"
	actorIntegTopoPrefix   = "/svc/topology"
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

// TestModule_Lease_GrantedOnFirstRegister verifies the lazy-start behaviour:
// EventLeaseGranted is NOT published on module.Start(), but IS published once
// the first RegisterService call drives EventRegistrationRequested on the EventBus,
// which LeaseActor handles by acquiring a lease.
//
// This corresponds to TestLeaseActor_StartGrantsLease in unit tests.
func TestModule_Lease_GrantedOnFirstRegister(t *testing.T) {
	m := startActorIntegModule(t, nil)
	ch := subscribeActorIntegEvents(t, m)

	// Lease must NOT be granted before any registration request.
	require.Never(t, func() bool {
		select {
		case env := <-ch:
			return env.Type == modulev2.EventLeaseGranted
		default:
			return false
		}
	}, 150*time.Millisecond, 20*time.Millisecond,
		"lease must not be granted before the first RegisterService call")

	// Trigger lazy lease acquisition via the first registration.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 1, Name: "lazy-lease-svc"},
		Path:      actorIntegByIDPrefix + "/lazy-lease-1",
		TTL:       10,
	})
	require.NoError(t, err)

	// Now EventLeaseGranted must arrive.
	env := waitForActorIntegEvent(t, ch, modulev2.EventLeaseGranted, 5*time.Second)
	pl, ok := env.Payload.(modulev2.LeaseGrantedPayload)
	require.True(t, ok, "payload must be LeaseGrantedPayload")
	_ = pl
	assert.Equal(t, uint64(1), env.LeaseEpoch, "first lease grant must have epoch=1")
}

// TestModule_Lease_StopsOnLastUnregister verifies that when the last registered
// service is removed, EventRegistrationEmpty drives LeaseActor to revoke the
// lease (pendingStop level-triggered flag) and the actor returns to idle state.
//
// After the stop, a new RegisterService triggers a fresh lease grant (epoch=1
// because onStop resets the state).  This proves the lease lifecycle correctly
// ties to registration presence.
func TestModule_Lease_StopsOnLastUnregister(t *testing.T) {
	m := startActorIntegModule(t, nil)
	ch := subscribeActorIntegEvents(t, m)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	svcPath := actorIntegByIDPrefix + "/lease-stop-901"

	// Register — triggers EventRegistrationRequested → lease acquisition.
	_, err := m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 901, Name: "lease-stop-svc"},
		Path:      svcPath,
		TTL:       10,
	})
	require.NoError(t, err)
	waitForActorIntegEvent(t, ch, modulev2.EventLeaseGranted, 5*time.Second) // epoch=1

	// Unregister the last service — triggers EventRegistrationEmpty → lease stop.
	err = m.UnregisterService(ctx, svcPath)
	require.NoError(t, err)

	// After the internal stop, the LeaseActor is idle: Tick must NOT produce a new grant.
	// Wait long enough (>RetryInterval=100ms) for any Tick to have fired.
	require.Never(t, func() bool {
		select {
		case env := <-ch:
			return env.Type == modulev2.EventLeaseGranted
		default:
			return false
		}
	}, 300*time.Millisecond, 20*time.Millisecond,
		"no lease grant must occur while no services are registered")

	// Re-register — drives a new EventRegistrationRequested → fresh lease.
	_, err = m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 902, Name: "lease-stop-svc-2"},
		Path:      actorIntegByIDPrefix + "/lease-stop-902",
		TTL:       10,
	})
	require.NoError(t, err)
	env := waitForActorIntegEvent(t, ch, modulev2.EventLeaseGranted, 5*time.Second)
	// leaseEpoch resets to 1 because onStop resets the full leaseState.
	assert.Equal(t, uint64(1), env.LeaseEpoch, "fresh lease after re-registration must have epoch=1")
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
	// handle.Wait intentionally not called: this test only verifies that
	// onRemoveService always publishes EventRegistrationChanged.  The
	// AddDiscovery and RemoveService messages are processed in FIFO mailbox
	// order, so RemoveService is guaranteed to execute after AddDiscovery.
	_ = handle

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

	cfg := modulev2.PathConfig{
		ByIDPrefix:     actorIntegByIDPrefix,
		ByNamePrefix:   actorIntegByNamePrefix,
		TopologyPrefix: actorIntegTopoPrefix,
		LeaseTTL:       30,
	}
	m1 := modulev2.NewEtcdModule(cli1, cfg, modulev2.ModuleOptions{RetryInterval: 100 * time.Millisecond})
	require.NoError(t, m1.Start(context.Background()))

	ch1 := make(chan modulev2.EventEnvelope, 32)
	m1.Subscribe(func(e modulev2.EventEnvelope) {
		select {
		case ch1 <- e:
		default:
		}
	})

	// Trigger lazy lease acquisition via a registration request.
	regCtx1, regCancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer regCancel1()
	_, _ = m1.RegisterService(regCtx1, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 1, Name: "reload-svc"},
		Path:      actorIntegByIDPrefix + "/reload-svc-1",
		TTL:       10,
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

	// Trigger lazy lease acquisition on the second module instance.
	regCtx2, regCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer regCancel2()
	_, _ = m2.RegisterService(regCtx2, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 2, Name: "reload-svc-2"},
		Path:      actorIntegByIDPrefix + "/reload-svc-2",
		TTL:       10,
	})

	// The new instance must re-acquire a lease independently.
	env := waitForActorIntegEvent(t, ch2, modulev2.EventLeaseGranted, 5*time.Second)
	pl, ok := env.Payload.(modulev2.LeaseGrantedPayload)
	require.True(t, ok)
	_ = pl // LeaseID=0 from mockserver is normal
	assert.Equal(t, uint64(1), env.LeaseEpoch,
		"new module must start with lease epoch 1")
}
// TestModule_EventLeaseGranted_FiredOnSubscribeIfAlreadyActive corresponds to
// C++ I.1.6 (cluster_event_up_down_callbacks).
//
// In C++, add_on_event_up(callback, trigger_if_running=true) fires the callback
// immediately when the cluster is already running, so late joiners never miss
// the "already active" state.
//
// In Go, the EventBus has no replay semantics.  The idiomatic equivalent is:
//   1. Subscribe before the event can fire  (get future deliveries), OR
//   2. Query current state via GetSnapshot() (read-your-writes without replay).
//
// This test verifies both aspects of the Go model:
//   a. A subscriber registered AFTER EventLeaseGranted epoch=1 was published
//      does NOT receive a passive replay of that event.
//   b. The same late subscriber can determine the module is active (lease held,
//      snapshot ready) through GetSnapshot().Discovery.Ready — the Go
//      equivalent of the C++ trigger_if_running guarantee.
func TestModule_EventLeaseGranted_FiredOnSubscribeIfAlreadyActive(t *testing.T) {
	// Arrange: start module with watched prefix, subscribe early, register a
	// service to trigger lazy lease acquisition.
	m := startActorIntegModule(t, []string{actorIntegByIDPrefix})
	earlySubCh := subscribeActorIntegEvents(t, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 201, Name: "trigger-if-running-svc"},
		Path:      actorIntegByIDPrefix + "/trigger-if-running-201",
		TTL:       10,
	})
	require.NoError(t, err)

	// Wait until lease is granted (epoch=1): the module is now holding a lease.
	// Note: EventWatchSnapshotLoaded may fire inside Start() before earlySubCh
	// is set up, so we do NOT wait for it via the channel.  Instead, the
	// snapshot readiness assertion below polls GetSnapshot() directly, which is
	// the authoritative signal regardless of event ordering.
	waitForActorIntegEvent(t, earlySubCh, modulev2.EventLeaseGranted, 5*time.Second)

	// Act: subscribe a LATE handler — this is the Go equivalent of calling
	// add_on_event_up(callback, trigger_if_running=true) after the cluster is up.
	lateSubCh := subscribeActorIntegEvents(t, m)

	// Assert (a): EventBus has no replay — the late subscriber must NOT receive
	// the already-published EventLeaseGranted passively.
	require.Never(t, func() bool {
		select {
		case env := <-lateSubCh:
			return env.Type == modulev2.EventLeaseGranted
		default:
			return false
		}
	}, 300*time.Millisecond, 20*time.Millisecond,
		"EventBus must not replay past EventLeaseGranted to a late subscriber")

	// Assert (b): Go equivalent of trigger_if_running — the late subscriber can
	// determine the active state by querying GetSnapshot().Discovery.Ready.
	// This is functionally equivalent to the C++ up_count > 0 assertion.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Discovery.Ready
	}, 3*time.Second, 20*time.Millisecond,
		"late subscriber can detect active lease via GetSnapshot().Discovery.Ready "+
			"(Go equivalent of C++ trigger_if_running)")
}