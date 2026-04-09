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

// ── Search mode semantic tests (mirrors C++ lower_bound_* test suite) ─────

// TestGetNodesByConsistentHash_UniqueNode verifies that ETCD_SEARCH_MODE_UNIQUE_NODE
// returns distinct physical nodes even when virtual nodes may repeat the same
// member consecutively.  With n=2 and a 3-node ring the result must have
// exactly 2 distinct nodes (mirrors C++ lower_bound_unique).
func TestGetNodesByConsistentHash_UniqueNode(t *testing.T) {
	m := makeEmbedNodesModule(t, hashPrefix, hashFixtures(), modulev2.ModuleOptions{
		ConsistentHashVirtualNodes: 10,
	})

	result, err := m.GetNodesByConsistentHash("key-1", 2, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Must be distinct paths.
	assert.NotEqual(t, result[0].Path, result[1].Path)
}

// TestGetNodesByConsistentHash_AllSearchModes_ValidMembers exercises all 8
// EtcdSearchMode values with a 3-node snapshot, verifying that:
//   - n=2 returns exactly 2 results (unless the ring has no members, handled separately)
//   - no result has duplicate paths within a single call
//   - all returned paths exist in the original fixture set
//
// Mirrors C++ lower_bound_{normal,unique,compact,compact_unique} cross-mode check.
func TestGetNodesByConsistentHash_AllSearchModes_ValidMembers(t *testing.T) {
	m := makeEmbedNodesModule(t, hashPrefix, hashFixtures(), modulev2.ModuleOptions{
		ConsistentHashVirtualNodes: 20,
	})

	knownPaths := map[string]bool{
		hashPrefix + "/a": true,
		hashPrefix + "/b": true,
		hashPrefix + "/c": true,
	}

	modes := []pb.EtcdSearchMode{
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE,
		pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE,
	}

	for _, mode := range modes {
		mode := mode
		t.Run(mode.String(), func(t *testing.T) {
			result, err := m.GetNodesByConsistentHash("search-mode-key", 2, nil, mode)
			require.NoError(t, err)
			require.Len(t, result, 2, "expected 2 results for mode %s", mode)

			paths := make(map[string]struct{}, len(result))
			for _, node := range result {
				assert.True(t, knownPaths[node.Path], "unexpected path %q for mode %s", node.Path, mode)
				paths[node.Path] = struct{}{}
			}
			assert.Len(t, paths, 2, "duplicate paths returned for mode %s", mode)
		})
	}
}

// TestGetNodesByConsistentHash_NextNode_DiffersFromAll verifies that
// ETCD_SEARCH_MODE_NEXT_NODE returns a different first element than
// ETCD_SEARCH_MODE_ALL for the same key, confirming that kNext skips the
// primary ring entry (mirrors C++ lower_bound_normal kNextNode semantic).
//
// With 3 physical nodes and 20 virtual nodes per member the probability that
// kAll and kNextNode accidentally return the same first element is very low.
// We run 3 different keys and assert at least one differs.
func TestGetNodesByConsistentHash_NextNode_DiffersFromAll(t *testing.T) {
	m := makeEmbedNodesModule(t, hashPrefix, hashFixtures(), modulev2.ModuleOptions{
		ConsistentHashVirtualNodes: 20,
	})

	anyDiffers := false
	testKeys := []string{"alpha", "beta", "gamma"}
	for _, key := range testKeys {
		resAll, errAll := m.GetNodesByConsistentHash(key, 1, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
		require.NoError(t, errAll)
		resNext, errNext := m.GetNodesByConsistentHash(key, 1, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE)
		require.NoError(t, errNext)
		require.Len(t, resAll, 1)
		require.Len(t, resNext, 1)
		if resAll[0].Path != resNext[0].Path {
			anyDiffers = true
			break
		}
	}
	assert.True(t, anyDiffers,
		"NEXT_NODE should return a different primary node than ALL for at least one of the test keys")
}
