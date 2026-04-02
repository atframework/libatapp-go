package integration_test

// Consistent-hash tests using real embed etcd.
//
// These replace etcd_module_v2/module_consistenthash_test.go which used
// makeModuleWithSnapshot — a helper that manually injected events into the
// bus and directly set EtcdModule internal fields.  Here every node enters
// the snapshot via the real Watch pipeline.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

const hashPrefix = "/test/hash"

// hashFixtures returns a three-node set used by most consistent-hash tests.
func hashFixtures() []DiscoveryFixture {
	return []DiscoveryFixture{
		{Path: hashPrefix + "/a", Discovery: &pb.AtappDiscovery{Id: 1, Name: "a"}},
		{Path: hashPrefix + "/b", Discovery: &pb.AtappDiscovery{Id: 2, Name: "b"}},
		{Path: hashPrefix + "/c", Discovery: &pb.AtappDiscovery{Id: 3, Name: "c"}},
	}
}

// TestGetNodesByConsistentHash_Basic confirms that n=2 returns exactly two
// distinct nodes from a three-node snapshot.
func TestGetNodesByConsistentHash_Basic(t *testing.T) {
	m := makeEmbedNodesModule(t, hashPrefix, hashFixtures(), modulev2.ModuleOptions{})

	result, err := m.GetNodesByConsistentHash("key-1", 2, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

// TestGetNodeByConsistentHash_Deterministic verifies that GetNodeByConsistentHash
// returns the same node for the same key on repeated calls (ring is stable).
func TestGetNodeByConsistentHash_Deterministic(t *testing.T) {
	m := makeEmbedNodesModule(t, hashPrefix, hashFixtures(), modulev2.ModuleOptions{
		ConsistentHashVirtualNodes: 80,
	})

	n1, err := m.GetNodeByConsistentHash("same-key", nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	n2, err := m.GetNodeByConsistentHash("same-key", nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)

	require.NotNil(t, n1)
	require.NotNil(t, n2)
	assert.Equal(t, n1.Path, n2.Path)
}

// TestGetNodesByConsistentHash_FilterByLabel verifies that only nodes matching
// the label selector are eligible for consistent-hash placement.
func TestGetNodesByConsistentHash_FilterByLabel(t *testing.T) {
	fixtures := []DiscoveryFixture{
		{
			Path: hashPrefix + "/a",
			Discovery: &pb.AtappDiscovery{
				Id:       1,
				Name:     "a",
				Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "prod"}},
			},
		},
		{
			Path: hashPrefix + "/b",
			Discovery: &pb.AtappDiscovery{
				Id:       2,
				Name:     "b",
				Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "test"}},
			},
		},
	}
	m := makeEmbedNodesModule(t, hashPrefix, fixtures, modulev2.ModuleOptions{})

	result, err := m.GetNodesByConsistentHash("key", 2, map[string]string{"env": "prod"}, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, hashPrefix+"/a", result[0].Path)
}

// TestGetNodesByConsistentHash_EmptySnapshot verifies that an error is returned
// when the snapshot is empty (watch loaded but no nodes were PUT).
func TestGetNodesByConsistentHash_EmptySnapshot(t *testing.T) {
	m := makeEmbedNodesModule(t, hashPrefix, nil, modulev2.ModuleOptions{})

	_, err := m.GetNodesByConsistentHash("key", 1, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	assert.Error(t, err)
}

// TestGetNodesByConsistentHash_FilterNoMatch verifies that an error is returned
// when the label filter matches zero nodes in the snapshot.
func TestGetNodesByConsistentHash_FilterNoMatch(t *testing.T) {
	fixtures := []DiscoveryFixture{
		{Path: hashPrefix + "/a", Discovery: &pb.AtappDiscovery{Id: 1, Name: "a"}},
	}
	m := makeEmbedNodesModule(t, hashPrefix, fixtures, modulev2.ModuleOptions{})

	_, err := m.GetNodesByConsistentHash("key", 1, map[string]string{"env": "prod"}, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	assert.Error(t, err)
}

// TestGetNodesByConsistentHash_NonPositiveN verifies that n=0 returns an empty
// slice without an error (requesting zero replicas is a valid no-op).
func TestGetNodesByConsistentHash_NonPositiveN(t *testing.T) {
	fixtures := []DiscoveryFixture{
		{Path: hashPrefix + "/a", Discovery: &pb.AtappDiscovery{Id: 1, Name: "a"}},
	}
	m := makeEmbedNodesModule(t, hashPrefix, fixtures, modulev2.ModuleOptions{})

	result, err := m.GetNodesByConsistentHash("key", 0, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	require.NoError(t, err)
	assert.Empty(t, result)
}
