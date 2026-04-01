package modulev2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

func makeModuleWithSnapshot(t *testing.T, nodes map[string]*snapshot.DiscoveryNode, opts ModuleOptions) (*EtcdModule, context.CancelFunc) {
	t.Helper()

	bus := runtime.NewEventBus()
	proj := orchestrator.NewProjectionActor(bus, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go proj.Run(ctx)

	// Wait for actor subscription to EventBus.
	time.Sleep(20 * time.Millisecond)

	if nodes == nil {
		nodes = map[string]*snapshot.DiscoveryNode{}
	}

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Nodes:    nodes,
			Revision: 1,
		},
	})

	require.Eventually(t, func() bool {
		snap := proj.GetSnapshot()
		return snap != nil
	}, time.Second, 10*time.Millisecond)

	return &EtcdModule{state: moduleStateRunning, opts: opts, projActor: proj}, cancel
}

func TestGetNodesByConsistentHash_Basic(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "a"}},
		"/svc/b": {Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 2, Name: "b"}},
		"/svc/c": {Path: "/svc/c", Info: &pb.AtappDiscovery{Id: 3, Name: "c"}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	result, err := m.GetNodesByConsistentHash("key-1", 2, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestGetNodeByConsistentHash_Deterministic(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "a"}},
		"/svc/b": {Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 2, Name: "b"}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{ConsistentHashVirtualNodes: 80})
	defer cancel()

	n1, err := m.GetNodeByConsistentHash("same-key", nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	n2, err := m.GetNodeByConsistentHash("same-key", nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)

	require.NotNil(t, n1)
	require.NotNil(t, n2)
	assert.Equal(t, n1.Path, n2.Path)
}

func TestGetNodesByConsistentHash_FilterByLabel(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "a", Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "prod"}}}},
		"/svc/b": {Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 2, Name: "b", Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "test"}}}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	result, err := m.GetNodesByConsistentHash("key", 2, map[string]string{"env": "prod"}, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "/svc/a", result[0].Path)
}

func TestGetNodesByConsistentHash_EmptySnapshot(t *testing.T) {
	m, cancel := makeModuleWithSnapshot(t, nil, ModuleOptions{})
	defer cancel()
	_, err := m.GetNodesByConsistentHash("key", 1, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	assert.Error(t, err)
}

func TestGetNodesByConsistentHash_FilterNoMatch(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "a"}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	_, err := m.GetNodesByConsistentHash("key", 1, map[string]string{"env": "prod"}, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	assert.Error(t, err)
}

func TestGetNodesByConsistentHash_NonPositiveN(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "a"}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	result, err := m.GetNodesByConsistentHash("key", 0, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	assert.Empty(t, result)
}
