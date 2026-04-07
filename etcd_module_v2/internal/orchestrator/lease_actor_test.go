package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mock/mockserver"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// ── shared test helpers ───────────────────────────────────────────────────

// startMockEtcd starts a single-node in-process etcd mock server and returns a
// connected clientv3.Client.  The server and client are cleaned up via t.Cleanup.
func startMockEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	servers, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	t.Cleanup(servers.Stop)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{servers.Servers[0].Address},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// waitForEvent blocks until an event of the expected type arrives or times out.
func waitForEvent(t *testing.T, ch <-chan runtime.EventEnvelope, want runtime.EventType, timeout time.Duration) runtime.EventEnvelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env, ok := <-ch:
			if !ok {
				t.Fatal("event channel closed before expected event")
			}
			if env.Type == want {
				return env
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event type %d", want)
			return runtime.EventEnvelope{}
		}
	}
}

// subscribeAll subscribes to all events and returns a buffered channel.
func subscribeAll(bus runtime.EventBus, bufCap int) (<-chan runtime.EventEnvelope, runtime.EventHandleHandle) {
	ch := make(chan runtime.EventEnvelope, bufCap)
	handle := bus.Subscribe(func(e runtime.EventEnvelope) {
		select {
		case ch <- e:
		default:
		}
	})
	return ch, handle
}

// ── LeaseActor tests ──────────────────────────────────────────────────────

// TestLeaseActor_StartGrantsLease verifies that actor.Start() causes the actor
// to Grant a lease and publish EventLeaseGranted on the bus.
func TestLeaseActor_StartGrantsLease(t *testing.T) {
	// Arrange
	cli := startMockEtcd(t)
	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 16)

	actor := orchestrator.NewLeaseActor(cli, bus, 30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act
	actor.Start(10)

	// Assert: EventLeaseGranted must arrive with epoch=1.
	// Note: mock/mockserver returns LeaseID=0 and TTL=0; we only assert the event fires.
	env := waitForEvent(t, evCh, runtime.EventLeaseGranted, 5*time.Second)
	_, ok := env.Payload.(orchestrator.LeaseGrantedPayload)
	require.True(t, ok, "payload type mismatch")
	assert.Equal(t, uint64(1), env.LeaseEpoch)
}

// TestLeaseActor_KeepaliveExpiry_TriggersRebuild verifies that when the active
// lease is externally revoked (simulating keepalive expiry / server eviction),
// the actor detects the expiry and automatically re-grants a new lease.
func TestLeaseActor_KeepaliveExpiry_TriggersRebuild(t *testing.T) {
	// Use two mock servers: the client holds both endpoints.
	// Stopping server1 kills the active KeepAlive stream, which the actor
	// interprets as expiry; gRPC then fails over to server2 for the rebuild Grant.
	servers1, err := mockserver.StartMockServers(1)
	require.NoError(t, err)

	servers2, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	defer servers2.Stop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{servers1.Servers[0].Address, servers2.Servers[0].Address},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 32)

	actor := orchestrator.NewLeaseActor(cli, bus, 30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act: acquire a lease (via server1), then kill server1 to close the keepalive stream.
	actor.Start(60)
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 10*time.Second)
	servers1.Stop() // forces keepalive channel close → leaseMsgExpired

	// Assert: actor must detect expiry, then rebuild on server2.
	waitForEvent(t, evCh, runtime.EventLeaseExpired, 10*time.Second)
	secondGrant := waitForEvent(t, evCh, runtime.EventLeaseGranted, 15*time.Second)
	assert.Equal(t, uint64(2), secondGrant.LeaseEpoch, "rebuilt lease must carry epoch=2")
}

// TestLeaseActor_Stop_CleanShutdown verifies that actor.Stop() returns without
// error and the actor goroutine terminates gracefully.
//
// Note: mock/mockserver returns LeaseID=0. The actor's onStop path only calls
// Revoke/publishLeaseReleased when leaseID != 0, so EventLeaseReleased is not
// expected here. This test validates the Stop lifecycle, not the revoke side-effect.
func TestLeaseActor_Stop_CleanShutdown(t *testing.T) {
	// Arrange
	cli := startMockEtcd(t)
	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 16)

	actor := orchestrator.NewLeaseActor(cli, bus, 30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	actor.Start(60)
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 5*time.Second)

	// Act: stop the actor and verify it completes without blocking.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	select {
	case stopErr := <-actor.Stop(stopCtx):
		require.NoError(t, stopErr)
	case <-stopCtx.Done():
		t.Fatal("Stop timed out")
	}
}

// TestLeaseActor_Tick_RetriesAcquiring verifies that when the initial Grant
// fails (server unavailable), a subsequent Tick drives a successful retry.
func TestLeaseActor_Tick_RetriesAcquiring(t *testing.T) {
	// Arrange: start a server, immediately stop it, then create a client pointing
	// to the now-dead address so the first Grant fails fast (connection refused).
	servers1, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	deadAddr := servers1.Servers[0].Address
	servers1.Stop() // port released; connection to deadAddr will be refused

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{deadAddr},
		DialTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 16)

	actor := orchestrator.NewLeaseActor(cli, bus, 30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act: Start — Grant fails because server is down.  Actor stays in Acquiring.
	// (tryGrant uses a 10 s internal timeout, but connection-refused is immediate.)
	actor.Start(10)
	// Give the actor goroutine time to process Start and observe the failed Grant.
	time.Sleep(300 * time.Millisecond)

	// Bring up a fresh server and redirect the client to it.
	servers2, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	defer servers2.Stop()
	cli.SetEndpoints(servers2.Servers[0].Address)

	// Tick triggers another tryGrant; this time the server is reachable.
	actor.Tick()

	// Assert: EventLeaseGranted must arrive (from the Tick-driven retry).
	// Note: mock/mockserver returns LeaseID=0; we only verify the event fires.
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 5*time.Second)
}

// TestLeaseActor_Post_NonBlocking_WhenMailboxFull verifies that a Tick() call
// never blocks even when the actor's mailbox is already full.
func TestLeaseActor_Post_NonBlocking_WhenMailboxFull(t *testing.T) {
	// Arrange: actor is NOT running — mailbox (cap=4) fills up quickly.
	cli := startMockEtcd(t)
	bus := runtime.NewEventBus()
	actor := orchestrator.NewLeaseActor(cli, bus, 30)

	// Fill the mailbox.
	for i := 0; i < 4; i++ {
		actor.Tick()
	}

	// A further Post must not block.
	postDone := make(chan struct{})
	go func() {
		actor.Tick() // must drop silently
		close(postDone)
	}()

	select {
	case <-postDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Tick blocked when mailbox was full")
	}
}
