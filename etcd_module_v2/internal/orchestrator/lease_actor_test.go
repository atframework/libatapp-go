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

// ── Mock EtcdClient ───────────────────────────────────────────────────────

type mockEtcdClient struct {
	grantFn     func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	keepAliveFn func(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	revokeFn    func(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	getFn       func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	putFn       func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	deleteFn    func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	watchFn     func(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

func (m *mockEtcdClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	if m.grantFn != nil {
		return m.grantFn(ctx, ttl)
	}
	return &clientv3.LeaseGrantResponse{ID: 1001, TTL: ttl}, nil
}

func (m *mockEtcdClient) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if m.keepAliveFn != nil {
		return m.keepAliveFn(ctx, id)
	}
	// By default: channel closes when ctx is done (simulates normal keepalive until stop).
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (m *mockEtcdClient) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	if m.revokeFn != nil {
		return m.revokeFn(ctx, id)
	}
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (m *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if m.getFn != nil {
		return m.getFn(ctx, key, opts...)
	}
	return &clientv3.GetResponse{}, nil
}

func (m *mockEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if m.putFn != nil {
		return m.putFn(ctx, key, val, opts...)
	}
	return &clientv3.PutResponse{}, nil
}

func (m *mockEtcdClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, key, opts...)
	}
	return &clientv3.DeleteResponse{}, nil
}

func (m *mockEtcdClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if m.watchFn != nil {
		return m.watchFn(ctx, key, opts...)
	}
	ch := make(chan clientv3.WatchResponse)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func (m *mockEtcdClient) Close() error {
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────

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
func subscribeAll(bus runtime.EventBus, cap int) (<-chan runtime.EventEnvelope, runtime.EventHandleHandle) {
	ch := make(chan runtime.EventEnvelope, cap)
	handle := bus.Subscribe(func(e runtime.EventEnvelope) {
		select {
		case ch <- e:
		default:
		}
	})
	return ch, handle
}

// ── LeaseActor tests ──────────────────────────────────────────────────────

func TestLeaseActor_StartGrantsLease(t *testing.T) {
	// Arrange
	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 16)

	mock := &mockEtcdClient{}
	actor := orchestrator.NewLeaseActor(mock, bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act
	actor.Start(10)

	// Assert: EventLeaseGranted must arrive
	env := waitForEvent(t, evCh, runtime.EventLeaseGranted, 3*time.Second)
	pl, ok := env.Payload.(orchestrator.LeaseGrantedPayload)
	require.True(t, ok, "payload type mismatch")
	assert.Equal(t, clientv3.LeaseID(1001), pl.LeaseID)
	assert.Equal(t, int64(10), pl.TTL)
	assert.Equal(t, uint64(1), env.LeaseEpoch)
}

func TestLeaseActor_KeepaliveExpiry_TriggersRebuild(t *testing.T) {
	// Arrange: keepalive channel is closed immediately to simulate expired lease.
	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 32)

	kaCh := make(chan *clientv3.LeaseKeepAliveResponse) // never sends; we'll close below

	var grantCount int
	mock := &mockEtcdClient{
		grantFn: func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
			grantCount++
			return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(1000 + int64(grantCount)), TTL: ttl}, nil
		},
		keepAliveFn: func(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			// Return the single controllable channel.
			return kaCh, nil
		},
	}
	actor := orchestrator.NewLeaseActor(mock, bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act: start lease, wait for first grant, then simulate expiry by closing kaCh.
	actor.Start(5)
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 3*time.Second)

	// Close the keepalive channel to trigger expiry.
	close(kaCh)

	// Assert: EventLeaseExpired then EventLeaseGranted (rebuild) must arrive.
	waitForEvent(t, evCh, runtime.EventLeaseExpired, 3*time.Second)
	// After expiry the actor immediately retries; the second keepaliveFn call
	// will arrive, but since kaCh is already closed we provide a new channel.
	// (In practice the rebuild path calls tryGrant → startKeepalive again with
	//  a new kaCtx, so the second keepaliveFn call should return a new channel.)
	// The second EventLeaseGranted confirms rebuild.
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 3*time.Second)
	assert.GreaterOrEqual(t, grantCount, 2, "Grant should have been called at least twice")
}

func TestLeaseActor_Stop_RevokesLease(t *testing.T) {
	// Arrange
	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 16)

	var revokeCalled bool
	mock := &mockEtcdClient{
		revokeFn: func(_ context.Context, _ clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
			revokeCalled = true
			return &clientv3.LeaseRevokeResponse{}, nil
		},
	}
	actor := orchestrator.NewLeaseActor(mock, bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Start and wait for lease grant.
	actor.Start(10)
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 3*time.Second)

	// Act: stop the actor.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	select {
	case err := <-actor.Stop(stopCtx):
		require.NoError(t, err)
	case <-stopCtx.Done():
		t.Fatal("Stop timed out")
	}

	// Assert: EventLeaseReleased must have been published, Revoke must be called.
	waitForEvent(t, evCh, runtime.EventLeaseReleased, 2*time.Second)
	assert.True(t, revokeCalled, "Revoke should have been called")
}

func TestLeaseActor_Tick_RetriesAcquiring(t *testing.T) {
	// Arrange: first Grant call fails, second succeeds.
	bus := runtime.NewEventBus()
	evCh, _ := subscribeAll(bus, 16)

	grantCount := 0
	mock := &mockEtcdClient{
		grantFn: func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
			grantCount++
			if grantCount == 1 {
				return nil, context.DeadlineExceeded // first attempt fails
			}
			return &clientv3.LeaseGrantResponse{ID: 2001, TTL: ttl}, nil
		},
	}
	actor := orchestrator.NewLeaseActor(mock, bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act: Start (first Grant fails silently), then Tick drives the retry.
	actor.Start(10)
	// Give the actor time to process Start (first Grant fails).
	time.Sleep(50 * time.Millisecond)
	actor.Tick()

	// Assert: EventLeaseGranted arrives on the second attempt.
	waitForEvent(t, evCh, runtime.EventLeaseGranted, 3*time.Second)
	assert.Equal(t, 2, grantCount)
}

func TestLeaseActor_Post_NonBlocking_WhenMailboxFull(t *testing.T) {
	// Arrange: actor not running — mailbox (cap=4) can fill up.
	bus := runtime.NewEventBus()
	mock := &mockEtcdClient{}
	actor := orchestrator.NewLeaseActor(mock, bus)

	// Fill the mailbox.
	for i := 0; i < 4; i++ {
		actor.Tick()
	}

	// Further Post must not block.
	postDone := make(chan struct{})
	go func() {
		actor.Tick() // should drop silently
		close(postDone)
	}()

	select {
	case <-postDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Tick blocked when mailbox was full")
	}
}

func TestLeaseActor_WithMockServer_ConnectionLifecycle(t *testing.T) {
	// Arrange: start mock etcd gRPC server and real clientv3.
	servers, err := mockserver.StartMockServers(1)
	require.NoError(t, err)
	defer servers.Stop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{servers.Servers[0].Address},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	bus := runtime.NewEventBus()
	evCh, handle := subscribeAll(bus, 32)
	defer bus.Unsubscribe(handle)

	actor := orchestrator.NewLeaseActor(cli, bus)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Act: acquire and then release lease using real client connection.
	actor.Start(5)
	granted := waitForEvent(t, evCh, runtime.EventLeaseGranted, 5*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	select {
	case stopErr := <-actor.Stop(stopCtx):
		require.NoError(t, stopErr)
	case <-stopCtx.Done():
		t.Fatal("Stop timed out")
	}

	// Assert
	grantPayload, ok := granted.Payload.(orchestrator.LeaseGrantedPayload)
	require.True(t, ok)
	_ = grantPayload
}
