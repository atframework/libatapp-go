package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

type registrationActorTestClient struct {
	mu      sync.Mutex
	puts    []registrationActorPutCall
	deletes []string
	failPut map[string]int
}

type registrationActorPutCall struct {
	key   string
	value string
	opts  []clientv3.OpOption
}

func (c *registrationActorTestClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return nil, nil
}

func (c *registrationActorTestClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failPut != nil {
		if remain := c.failPut[key]; remain > 0 {
			c.failPut[key] = remain - 1
			return nil, fmt.Errorf("forced put failure: %s", key)
		}
	}
	c.puts = append(c.puts, registrationActorPutCall{key: key, value: val, opts: opts})
	return &clientv3.PutResponse{}, nil
}

func (c *registrationActorTestClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, key)
	return &clientv3.DeleteResponse{}, nil
}

func (c *registrationActorTestClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return nil
}

func (c *registrationActorTestClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return nil, nil
}

func (c *registrationActorTestClient) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return nil, nil
}

func (c *registrationActorTestClient) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (c *registrationActorTestClient) SetEndpoints(_ ...string) {}

func (c *registrationActorTestClient) Close() error {
	return nil
}

func (c *registrationActorTestClient) putKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.puts))
	for _, call := range c.puts {
		out = append(out, call.key)
	}
	return out
}

func (c *registrationActorTestClient) deleteKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deletes...)
}

func (c *registrationActorTestClient) countPutKey(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, call := range c.puts {
		if call.key == key {
			count++
		}
	}
	return count
}

func publishLeaseGranted(bus runtime.EventBus, leaseID clientv3.LeaseID, epoch uint64) {
	bus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventLeaseGranted,
		Version:    1,
		Source:     runtime.EventSourceLeaseActor,
		LeaseEpoch: epoch,
		OccurredAt: time.Now(),
		Payload: LeaseGrantedPayload{
			LeaseID: leaseID,
		},
	})
}

func publishLeaseExpired(bus runtime.EventBus, epoch uint64) {
	bus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventLeaseExpired,
		Version:    1,
		Source:     runtime.EventSourceLeaseActor,
		LeaseEpoch: epoch,
		OccurredAt: time.Now(),
		Payload:    LeaseExpiredPayload{},
	})
}

func grantLeaseAndWait(t *testing.T, actor *RegistrationActor, client *registrationActorTestClient, bus runtime.EventBus, leaseID clientv3.LeaseID, epoch uint64) {
	t.Helper()
	probePath := fmt.Sprintf("/service/by_path/__lease_probe_%d", leaseID)
	probeName := fmt.Sprintf("__lease_probe_%d", leaseID)
	probeDone := actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{Id: uint64(leaseID), Name: probeName}, probePath, 16)
	require.NoError(t, <-probeDone)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		publishLeaseGranted(bus, leaseID, epoch)
		if client.countPutKey(probePath) >= 1 {
			_ = <-actor.RemoveService(context.Background(), probePath)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("wait timeout: lease grant not observed, leaseID=%d", leaseID)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("wait timeout: %s", message)
}

func TestRegistrationActor_Run_AddDiscoveryAndTopology(t *testing.T) {
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	regChangedCh := make(chan runtime.EventEnvelope, 8)
	h := bus.SubscribeType(runtime.EventRegistrationChanged, func(env runtime.EventEnvelope) {
		regChangedCh <- env
	})
	defer bus.Unsubscribe(h)

	grantLeaseAndWait(t, actor, client, bus, 1001, 1)

	discoveryDone := actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{
		Id:       42,
		Name:     "svc-a",
		Hostname: "host-a",
		Pid:      88,
		Identity: "identity-a",
		Version:  "1.0.0",
	}, "/service/by_path/svc-a-42", 16)
	require.NoError(t, <-discoveryDone)

	topologyDone := actor.AddTopology(context.Background(), &pb.AtappTopologyInfo{
		Id:       42,
		Name:     "svc-a",
		Hostname: "host-a",
		Pid:      88,
		Identity: "identity-a",
		Version:  "1.0.0",
	}, "/service/by_path/svc-a-42", 16)
	require.NoError(t, <-topologyDone)

	waitUntil(t, time.Second, func() bool {
		keys := client.putKeys()
		return len(keys) >= 4
	}, "expected 4 put operations")

	keys := client.putKeys()
	assert.Contains(t, keys, "/service/by_path/svc-a-42")
	assert.Contains(t, keys, "/service/by_name/svc-a-42")
	assert.Contains(t, keys, "/service/by_id/svc-a-42")
	assert.Contains(t, keys, "/service/topology/svc-a-42")

	deadline := time.After(time.Second)
	found := false
	for !found {
		select {
		case env := <-regChangedCh:
			payload, ok := env.Payload.(RegistrationChangedPayload)
			require.True(t, ok)
			if payload.SelfRegistrationSnapshot.LeaseEpoch != 1 {
				continue
			}
			_, found = payload.SelfRegistrationSnapshot.ByPath["/service/by_path/svc-a-42"]
		case <-deadline:
			t.Fatal("expected registration changed event for svc-a")
		}
	}
}

func TestRegistrationActor_Run_RemoveService_DeletesDiscoveryAndTopologyKeys(t *testing.T) {
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 1002, 1)

	require.NoError(t, <-actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{Id: 7, Name: "svc-b"}, "/service/by_path/svc-b-7", 16))
	require.NoError(t, <-actor.AddTopology(context.Background(), &pb.AtappTopologyInfo{Id: 700, Name: "topology-b"}, "/service/by_path/svc-b-7", 16))

	require.NoError(t, <-actor.RemoveService(context.Background(), "/service/by_path/svc-b-7"))

	waitUntil(t, time.Second, func() bool {
		deleted := client.deleteKeys()
		return len(deleted) >= 4
	}, "expected 4 delete operations")

	deleted := client.deleteKeys()
	assert.Contains(t, deleted, "/service/by_path/svc-b-7")
	assert.Contains(t, deleted, "/service/by_name/svc-b-7")
	assert.Contains(t, deleted, "/service/by_id/svc-b-7")
	assert.Contains(t, deleted, "/service/topology/topology-b-700")
}

func TestRegistrationActor_Run_LeaseReplay_RewritesStaleServices(t *testing.T) {
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 2001, 1)
	require.NoError(t, <-actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{Id: 10, Name: "svc-c"}, "/service/by_path/svc-c-10", 16))
	require.NoError(t, <-actor.AddTopology(context.Background(), &pb.AtappTopologyInfo{Id: 10, Name: "svc-c"}, "/service/by_path/svc-c-10", 16))

	waitUntil(t, time.Second, func() bool { return len(client.putKeys()) >= 4 }, "initial puts")

	publishLeaseExpired(bus, 2)
	grantLeaseAndWait(t, actor, client, bus, 2002, 3)

	waitUntil(t, time.Second, func() bool { return len(client.putKeys()) >= 8 }, "replay puts after lease rebuild")
	assert.GreaterOrEqual(t, client.countPutKey("/service/by_path/svc-c-10"), 2)
	assert.GreaterOrEqual(t, client.countPutKey("/service/topology/svc-c-10"), 2)
}

func TestRegistrationActor_Run_PutFailureThenReplayRecovers(t *testing.T) {
	client := &registrationActorTestClient{
		failPut: map[string]int{
			"/service/by_path/svc-d-11": 1,
		},
	}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 3001, 1)
	err := <-actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{Id: 11, Name: "svc-d"}, "/service/by_path/svc-d-11", 16)
	require.Error(t, err)
	assert.Equal(t, 0, client.countPutKey("/service/by_path/svc-d-11"))

	grantLeaseAndWait(t, actor, client, bus, 3002, 2)
	waitUntil(t, time.Second, func() bool {
		return client.countPutKey("/service/by_path/svc-d-11") >= 1
	}, "discovery replay should recover after next lease grant")
}

func TestRegistrationActor_Run_FlushTopology_RewritesRegisteredTopology(t *testing.T) {
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 4001, 1)
	require.NoError(t, <-actor.AddTopology(context.Background(), &pb.AtappTopologyInfo{Id: 900, Name: "topology-only"}, "/service/by_path/topology-only", 16))

	waitUntil(t, time.Second, func() bool { return len(client.putKeys()) >= 1 }, "initial topology put")
	before := len(client.putKeys())

	require.NoError(t, actor.FlushTopology(context.Background()))

	waitUntil(t, time.Second, func() bool { return len(client.putKeys()) > before }, "flush topology should trigger another put")
}

// ── D12 tests ─────────────────────────────────────────────────────────────

// TestRegistrationActor_Replay_NServicesEmitOneEvent verifies that when N
// discovery + topology services are all stale after a lease rebuild, exactly
// one RegistrationChanged event is emitted (not N events).
func TestRegistrationActor_Replay_NServicesEmitOneEvent(t *testing.T) {
	// Arrange: use epoch-1 grant only to ensure the actor goroutine is running.
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 6001, 1)

	for i := uint64(1); i <= 3; i++ {
		path := fmt.Sprintf("/service/by_path/svc-%d", i)
		require.NoError(t, <-actor.AddDiscovery(context.Background(),
			&pb.AtappDiscovery{Id: i, Name: fmt.Sprintf("svc-%d", i)}, path, 16))
		require.NoError(t, <-actor.AddTopology(context.Background(),
			&pb.AtappTopologyInfo{Id: i, Name: fmt.Sprintf("svc-%d", i)}, path, 16))
	}

	// Subscribe AFTER epoch-1 setup so we only count epoch-2 events.
	regChangedCh := make(chan runtime.EventEnvelope, 32)
	h := bus.SubscribeType(runtime.EventRegistrationChanged, func(env runtime.EventEnvelope) {
		regChangedCh <- env
	})
	defer bus.Unsubscribe(h)

	// Act: simulate lease expiry + single new LeaseGranted (no probe loop).
	publishLeaseExpired(bus, 1)
	publishLeaseGranted(bus, 6002, 2)

	// Wait for all 3 discovery services to be re-put.
	waitUntil(t, 2*time.Second, func() bool {
		return client.countPutKey("/service/by_path/svc-1") >= 2 &&
			client.countPutKey("/service/by_path/svc-2") >= 2 &&
			client.countPutKey("/service/by_path/svc-3") >= 2
	}, "all 3 discovery services replayed under epoch-2 lease")

	// Brief window for any extra events to arrive.
	time.Sleep(100 * time.Millisecond)

	// Assert: exactly 1 RegistrationChanged for epoch 2.
	epoch2Count := 0
	for {
		select {
		case env := <-regChangedCh:
			if env.LeaseEpoch == 2 {
				epoch2Count++
			}
		default:
			goto doneN
		}
	}
doneN:
	assert.Equal(t, 1, epoch2Count, "expected exactly 1 RegistrationChanged event for epoch-2 replay, got %d", epoch2Count)
}

// ── E3 tests ──────────────────────────────────────────────────────────────

// TestRegistrationActor_Snapshot_IncludesTopology verifies that after a
// successful AddTopology call, the RegistrationChangedPayload snapshot
// contains the topology entry in TopologyServices.
func TestRegistrationActor_Snapshot_IncludesTopology(t *testing.T) {
	// Arrange
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	regChangedCh := make(chan runtime.EventEnvelope, 16)
	h := bus.SubscribeType(runtime.EventRegistrationChanged, func(env runtime.EventEnvelope) {
		regChangedCh <- env
	})
	defer bus.Unsubscribe(h)

	grantLeaseAndWait(t, actor, client, bus, 5001, 1)

	// Act
	require.NoError(t, <-actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{
		Id: 10, Name: "svc-e",
	}, "/service/by_path/svc-e-10", 16))
	require.NoError(t, <-actor.AddTopology(context.Background(), &pb.AtappTopologyInfo{
		Id: 10, Name: "svc-e",
	}, "/service/by_path/svc-e-10", 16))

	// Assert: wait for an event whose snapshot contains the topology entry
	topoKey := "/service/topology/svc-e-10"
	deadline := time.After(time.Second)
	for {
		select {
		case env := <-regChangedCh:
			payload, ok := env.Payload.(RegistrationChangedPayload)
			require.True(t, ok)
			if _, found := payload.SelfRegistrationSnapshot.TopologyServices[topoKey]; found {
				// topology is present in snapshot — pass
				return
			}
		case <-deadline:
			t.Fatalf("snapshot never contained TopologyServices[%q]", topoKey)
		}
	}
}

// ── B2 tests ──────────────────────────────────────────────────────────────

// TestRegistrationActor_FlushTopology_PropagatesWriteError verifies that when
// a topology put fails during FlushTopology, the error is returned to the caller.
func TestRegistrationActor_FlushTopology_PropagatesWriteError(t *testing.T) {
	// Arrange: single topology service that is registered successfully.
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 8001, 1)

	topoPath := "/service/by_path/svc-b2"
	require.NoError(t, <-actor.AddDiscovery(context.Background(),
		&pb.AtappDiscovery{Id: 1, Name: "svc-b2"}, topoPath, 16))
	require.NoError(t, <-actor.AddTopology(context.Background(),
		&pb.AtappTopologyInfo{Id: 1, Name: "svc-b2"}, topoPath, 16))

	// The topology key written by buildTopologyKey is stored in
	// topologyServices; inject a failure for it.
	topoKey := "/service/topology/svc-b2-1"
	client.mu.Lock()
	client.failPut = map[string]int{topoKey: 1}
	client.mu.Unlock()

	// Act: FlushTopology should hit the injected failure.
	err := actor.FlushTopology(context.Background())

	// Assert: error propagated back.
	assert.Error(t, err, "FlushTopology should return error when topology put fails")
}

// TestRegistrationActor_FlushTopology_ReturnsNilOnSuccess verifies that when
// all topology puts succeed, FlushTopology returns nil.
func TestRegistrationActor_FlushTopology_ReturnsNilOnSuccess(t *testing.T) {
	// Arrange
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	grantLeaseAndWait(t, actor, client, bus, 8002, 1)

	topoPath := "/service/by_path/svc-b2-ok"
	require.NoError(t, <-actor.AddDiscovery(context.Background(),
		&pb.AtappDiscovery{Id: 2, Name: "svc-b2-ok"}, topoPath, 16))
	require.NoError(t, <-actor.AddTopology(context.Background(),
		&pb.AtappTopologyInfo{Id: 2, Name: "svc-b2-ok"}, topoPath, 16))

	// Act: FlushTopology with no failures.
	err := actor.FlushTopology(context.Background())

	// Assert
	assert.NoError(t, err, "FlushTopology should return nil when all puts succeed")
}

// ── B3 tests ──────────────────────────────────────────────────────────────

// TestRegistrationActor_FlushTopology_ReturnsErrNoLease_WhenNoActiveLease
// verifies that FlushTopology returns ErrNoLease when the actor holds no lease.
func TestRegistrationActor_FlushTopology_ReturnsErrNoLease_WhenNoActiveLease(t *testing.T) {
	// Arrange: actor started but no LeaseGranted ever published.
	client := &registrationActorTestClient{}
	bus := runtime.NewEventBus()
	actor := NewRegistrationActor(client, bus, "/service/by_name/", "/service/by_id/", "/service/topology")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go actor.Run(ctx)

	// Enqueue a discovery so the mailbox goroutine is confirmed started.
	_ = <-actor.AddDiscovery(context.Background(), &pb.AtappDiscovery{Id: 9, Name: "probe"}, "/service/probe", 16)

	// Act: no lease granted — FlushTopology must return ErrNoLease.
	err := actor.FlushTopology(context.Background())

	// Assert
	assert.ErrorIs(t, err, ErrNoLease)
}

// TestRegistrationActor_Snapshot_Clone_IncludesTopology verifies that
// SelfRegistrationSnapshot.Clone() copies TopologyServices correctly.
func TestRegistrationActor_Snapshot_Clone_IncludesTopology(t *testing.T) {
	// Arrange
	orig := snapshot.SelfRegistrationSnapshot{
		LeaseID:    42,
		LeaseEpoch: 1,
		ByPath:     map[string]*pb.AtappDiscovery{"/by/path": {Id: 1}},
		TopologyServices: map[string]*pb.AtappTopologyInfo{
			"/topology/svc-1": {Id: 1, Name: "svc"},
		},
	}

	// Act
	cloned := orig.Clone()

	// Assert
	require.NotNil(t, cloned.TopologyServices)
	assert.Equal(t, len(orig.TopologyServices), len(cloned.TopologyServices))
	assert.Equal(t, orig.TopologyServices["/topology/svc-1"], cloned.TopologyServices["/topology/svc-1"])
	// Mutation isolation: modifying clone does not affect original
	delete(cloned.TopologyServices, "/topology/svc-1")
	assert.Len(t, orig.TopologyServices, 1)
}
