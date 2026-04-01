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
			if payload.RegistrationSnapshot.LeaseEpoch != 1 {
				continue
			}
			_, found = payload.RegistrationSnapshot.ByPath["/service/by_path/svc-a-42"]
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
