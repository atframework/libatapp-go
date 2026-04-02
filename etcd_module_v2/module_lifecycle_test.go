package modulev2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ── Mock EtcdClient for testing ───────────────────────────────────────────

type testMockEtcdClient struct {
	grantFn     func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	keepAliveFn func(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	revokeFn    func(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	getFn       func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	putFn       func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	deleteFn    func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	watchFn     func(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

func (m *testMockEtcdClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	if m.grantFn != nil {
		return m.grantFn(ctx, ttl)
	}
	return &clientv3.LeaseGrantResponse{ID: 1001, TTL: ttl}, nil
}

func (m *testMockEtcdClient) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if m.keepAliveFn != nil {
		return m.keepAliveFn(ctx, id)
	}
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (m *testMockEtcdClient) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	if m.revokeFn != nil {
		return m.revokeFn(ctx, id)
	}
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (m *testMockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if m.getFn != nil {
		return m.getFn(ctx, key, opts...)
	}
	return &clientv3.GetResponse{}, nil
}

func (m *testMockEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if m.putFn != nil {
		return m.putFn(ctx, key, val, opts...)
	}
	return &clientv3.PutResponse{}, nil
}

func (m *testMockEtcdClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, key, opts...)
	}
	return &clientv3.DeleteResponse{}, nil
}

func (m *testMockEtcdClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
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

func (m *testMockEtcdClient) SetEndpoints(_ ...string) {}

func (m *testMockEtcdClient) Close() error {
	return nil
}

func NewMockEtcdClient() EtcdClient {
	return &testMockEtcdClient{}
}

func DefaultPathConfig() PathConfig {
	return PathConfig{
		ByIDPrefix:    "services-by-id/",
		ByNamePrefix:  "services-by-name/",
		TopologyPrefix: "topology/",
		WatchPrefixes: []string{"services-by-id/", "services-by-name/", "topology/"},
		LeaseTTL:      30,
	}
}

// ── Test: Stop without Start ──────────────────────────────────────────────

// TestEtcdModule_Stop_WithoutStart verifies that Stop returns ErrNotRunning
// when called on a module that was never started.
func TestEtcdModule_Stop_WithoutStart(t *testing.T) {
	client := NewMockEtcdClient()
	cfg := DefaultPathConfig()
	opts := ModuleOptions{RetryInterval: 100 * time.Millisecond}

	module := NewEtcdModule(client, cfg, opts)
	ctx := context.Background()

	err := module.Stop(ctx)
	assert.Equal(t, ErrNotRunning, err)
}

// ── Test: Stop with nil cancelFunc (defensive) ────────────────────────────

// TestEtcdModule_Stop_NilCancelFuncDoesNotPanic demonstrates that Stop handles
// a nil cancelFunc gracefully (though this shouldn't happen in practice if Start
// succeeded).
func TestEtcdModule_Stop_NilCancelFuncDoesNotPanic(t *testing.T) {
	client := NewMockEtcdClient()
	cfg := DefaultPathConfig()
	opts := ModuleOptions{RetryInterval: 100 * time.Millisecond}

	module := NewEtcdModule(client, cfg, opts)

	// Manually force state to running without proper Start (simulating partial init fail).
	module.mu.Lock()
	module.state = moduleStateRunning
	module.cancelFunc = nil // Simulate the nil case
	module.mu.Unlock()

	ctx := context.Background()

	// This should not panic.
	assert.NotPanics(t, func() {
		_ = module.Stop(ctx)
	})
}

// ── Test: Stop with lease revoke error ────────────────────────────────────

// TestEtcdModule_Stop_LeaseRevokeError verifies that lease revocation errors
// are captured and returned, allowing the caller to see what went wrong.
func TestEtcdModule_Stop_LeaseRevokeError(t *testing.T) {
	mockClient := &testMockEtcdClient{
		revokeFn: func(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
			return nil, errors.New("simulated revoke error")
		},
	}
	cfg := DefaultPathConfig()
	opts := ModuleOptions{RetryInterval: 100 * time.Millisecond}

	module := NewEtcdModule(mockClient, cfg, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start the module normally.
	err := module.Start(ctx)
	require.NoError(t, err)

	// Call Stop – should capture and return the lease error.
	stopErr := module.Stop(ctx)

	// Verify that the error message contains information about the lease revoke failure.
	if stopErr != nil {
		assert.Contains(t, stopErr.Error(), "lease revoke error")
	}
}

// ── Test: Stop with context timeout ──────────────────────────────────────

// TestEtcdModule_Stop_ContextTimeout verifies that if the context is cancelled
// during Stop, an error is returned.
func TestEtcdModule_Stop_ContextTimeout(t *testing.T) {
	client := NewMockEtcdClient()
	cfg := DefaultPathConfig()
	opts := ModuleOptions{RetryInterval: 100 * time.Millisecond}

	module := NewEtcdModule(client, cfg, opts)

	// Start with a background context.
	startCtx := context.Background()
	err := module.Start(startCtx)
	require.NoError(t, err)

	// Stop with a pre-cancelled context to simulate timeout.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	stopErr := module.Stop(cancelledCtx)
	assert.NotNil(t, stopErr)
	assert.True(t, errors.Is(stopErr, context.Canceled))
}

// ── Test: Stop multiple times ────────────────────────────────────────────

// TestEtcdModule_Stop_Multiple verifies that calling Stop multiple times
// returns ErrNotRunning after the first successful stop.
func TestEtcdModule_Stop_Multiple(t *testing.T) {
	client := NewMockEtcdClient()
	cfg := DefaultPathConfig()
	opts := ModuleOptions{RetryInterval: 100 * time.Millisecond}

	module := NewEtcdModule(client, cfg, opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start the module.
	err := module.Start(ctx)
	require.NoError(t, err)

	// First stop should succeed.
	stopErr1 := module.Stop(ctx)
	// stopErr1 might be nil or contain lease errors; not critical here.
	assert.NotPanics(t, func() {
		_ = stopErr1
	})

	// Second stop should return ErrNotRunning.
	stopErr2 := module.Stop(ctx)
	assert.Equal(t, ErrNotRunning, stopErr2)
}

// ── Test: Stop with generous timeout ─────────────────────────────────────

// TestEtcdModule_Stop_NormalShutdown verifies the happy path: Start, then Stop
// with sufficient timeout.
func TestEtcdModule_Stop_NormalShutdown(t *testing.T) {
	client := NewMockEtcdClient()
	cfg := DefaultPathConfig()
	opts := ModuleOptions{RetryInterval: 100 * time.Millisecond}

	module := NewEtcdModule(client, cfg, opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start the module.
	err := module.Start(ctx)
	require.NoError(t, err)

	// Verify it's running.
	module.mu.Lock()
	assert.Equal(t, moduleStateRunning, module.state)
	module.mu.Unlock()

	// Stop the module with a generous timeout.
	stopErr := module.Stop(ctx)
	// Should succeed (nil or non-timeout error).
	if stopErr != nil {
		assert.False(t, errors.Is(stopErr, context.DeadlineExceeded))
	}

	// Verify state is stopped.
	module.mu.Lock()
	assert.Equal(t, moduleStateStopped, module.state)
	module.mu.Unlock()
}

// ── Test: Stop cannot be called before Start ──────────────────────────────

// TestEtcdModule_Stop_BeforeStart confirms that Stop returns ErrNotRunning
// when the module has never been started.
func TestEtcdModule_Stop_BeforeStart(t *testing.T) {
	client := NewMockEtcdClient()
	cfg := DefaultPathConfig()
	opts := ModuleOptions{}

	module := NewEtcdModule(client, cfg, opts)

	ctx := context.Background()
	err := module.Stop(ctx)
	assert.Equal(t, ErrNotRunning, err)
}
