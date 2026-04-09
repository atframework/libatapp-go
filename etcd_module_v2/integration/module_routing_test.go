package integration_test

// Routing tests using real embed etcd.
//
// These replace etcd_module_v2/module_routing_test.go which used
// makeModuleWithSnapshot — a helper that manually injected
// EventWatchSnapshotLoaded into the event bus and hard-wired EtcdModule
// internal fields.  Here every node in the snapshot arrives through the
// real Watch pipeline: external client PUT → etcd → WatchActor stream →
// EventWatchNodeUp → ProjectionActor snapshot update.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

const routingPrefix = "/test/route"

// routingFixtures returns a deterministic set of discovery nodes used by the
// routing tests.  All paths live under routingPrefix so the module's single
// watch stream delivers them without extra configuration.
func routingFixtures() []DiscoveryFixture {
	return []DiscoveryFixture{
		{
			Path:      routingPrefix + "/a",
			Discovery: &pb.AtappDiscovery{Id: 1, Name: "svc-a", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 0}},
		},
		{
			Path:      routingPrefix + "/b",
			Discovery: &pb.AtappDiscovery{Id: 2, Name: "svc-b", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 1}},
		},
		{
			Path:      routingPrefix + "/c",
			Discovery: &pb.AtappDiscovery{Id: 3, Name: "svc-c", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 2}},
		},
	}
}

// TestGetNodeByID verifies that a node can be looked up by its discovery ID
// after the snapshot is populated via a real embed etcd Watch stream.
func TestGetNodeByID(t *testing.T) {
	m := makeEmbedNodesModule(t, routingPrefix, routingFixtures(), modulev2.ModuleOptions{})

	node := m.GetNodeByID(2)
	require.NotNil(t, node)
	assert.Equal(t, routingPrefix+"/b", node.Path)

	assert.Nil(t, m.GetNodeByID(999))
}

// TestGetNodeByName verifies lookup by service name after snapshot population.
func TestGetNodeByName(t *testing.T) {
	m := makeEmbedNodesModule(t, routingPrefix, routingFixtures(), modulev2.ModuleOptions{})

	node := m.GetNodeByName("svc-a")
	require.NotNil(t, node)
	assert.Equal(t, routingPrefix+"/a", node.Path)

	assert.Nil(t, m.GetNodeByName("svc-missing"))
}

// TestGetNodeByRoundRobin_Order verifies that round-robin iterates nodes in
// ascending StatefulPodIndex order regardless of insertion order in the snapshot.
func TestGetNodeByRoundRobin_Order(t *testing.T) {
	// Put nodes in reverse order to confirm ordering is by StatefulPodIndex,
	// not by insertion / etcd revision.
	fixtures := []DiscoveryFixture{
		{Path: routingPrefix + "/c", Discovery: &pb.AtappDiscovery{Id: 3, Name: "svc-c", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 2}}},
		{Path: routingPrefix + "/a", Discovery: &pb.AtappDiscovery{Id: 1, Name: "svc-a", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 0}}},
		{Path: routingPrefix + "/b", Discovery: &pb.AtappDiscovery{Id: 2, Name: "svc-b", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 1}}},
	}
	m := makeEmbedNodesModule(t, routingPrefix, fixtures, modulev2.ModuleOptions{})

	n1, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	n2, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	n3, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)

	assert.Equal(t, routingPrefix+"/a", n1.Path, "first round-robin: StatefulPodIndex 0")
	assert.Equal(t, routingPrefix+"/b", n2.Path, "second round-robin: StatefulPodIndex 1")
	assert.Equal(t, routingPrefix+"/c", n3.Path, "third round-robin: StatefulPodIndex 2")
}

// TestGetNodeByRandom_Filter verifies that label-based filtering returns only
// matching nodes when multiple nodes are present in the snapshot.
func TestGetNodeByRandom_Filter(t *testing.T) {
	fixtures := []DiscoveryFixture{
		{
			Path: routingPrefix + "/prod",
			Discovery: &pb.AtappDiscovery{
				Id:   1,
				Name: "svc",
				Metadata: &pb.AtappMetadata{
					Labels: map[string]string{"env": "prod"},
				},
			},
		},
		{
			Path: routingPrefix + "/test",
			Discovery: &pb.AtappDiscovery{
				Id:   2,
				Name: "svc-test",
				Metadata: &pb.AtappMetadata{
					Labels: map[string]string{"env": "test"},
				},
			},
		},
	}
	m := makeEmbedNodesModule(t, routingPrefix, fixtures, modulev2.ModuleOptions{})

	node, err := m.GetNodeByRandom(map[string]string{"env": "prod"})
	require.NoError(t, err)
	require.NotNil(t, node)
	assert.Equal(t, routingPrefix+"/prod", node.Path)
}

// TestGetNodeByRoundRobin_Wrap verifies that round-robin wraps back to the
// first node after cycling through all N nodes (mirrors C++ round_robin test).
func TestGetNodeByRoundRobin_Wrap(t *testing.T) {
	fixtures := []DiscoveryFixture{
		{Path: routingPrefix + "/a", Discovery: &pb.AtappDiscovery{Id: 1, Name: "svc-a", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 0}}},
		{Path: routingPrefix + "/b", Discovery: &pb.AtappDiscovery{Id: 2, Name: "svc-b", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 1}}},
		{Path: routingPrefix + "/c", Discovery: &pb.AtappDiscovery{Id: 3, Name: "svc-c", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 2}}},
	}
	m := makeEmbedNodesModule(t, routingPrefix, fixtures, modulev2.ModuleOptions{})

	// First full cycle: a → b → c
	n1, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	n2, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	n3, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)

	assert.Equal(t, routingPrefix+"/a", n1.Path)
	assert.Equal(t, routingPrefix+"/b", n2.Path)
	assert.Equal(t, routingPrefix+"/c", n3.Path)

	// N+1th call must wrap back to the first node (StatefulPodIndex 0).
	n4, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	require.NotNil(t, n4)
	assert.Equal(t, n1.Path, n4.Path, "round-robin must wrap back to first node after full cycle")
}

// TestGetNodeByRandom_Empty verifies that GetNodeByRandom returns an error when
// the snapshot contains no nodes (mirrors C++ discovery_empty_set_operations).
func TestGetNodeByRandom_Empty(t *testing.T) {
	m := makeEmbedNodesModule(t, routingPrefix, nil, modulev2.ModuleOptions{})

	node, err := m.GetNodeByRandom(nil)
	assert.Nil(t, node)
	assert.Error(t, err)
}

// TestGetNodeByRoundRobin_Empty verifies that GetNodeByRoundRobin returns an
// error when the snapshot contains no nodes (watch loaded, but etcd is empty).
func TestGetNodeByRoundRobin_Empty(t *testing.T) {
	m := makeEmbedNodesModule(t, routingPrefix, nil, modulev2.ModuleOptions{})

	node, err := m.GetNodeByRoundRobin(nil)
	assert.Nil(t, node)
	assert.Error(t, err)
}
