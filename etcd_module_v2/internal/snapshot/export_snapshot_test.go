package snapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/etcdversion"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// ── ExportSnapshot ────────────────────────────────────────────────────────

func TestExportSnapshot_Empty(t *testing.T) {
	assert.True(t, (*ExportSnapshot)(nil).Empty())
	assert.True(t, (&ExportSnapshot{}).Empty())
	assert.False(t, (&ExportSnapshot{Version: 1}).Empty())
}

// TestExportSnapshot_Clone_Isolation verifies that Clone produces an independent
// read-model: mutating the clone's maps does not affect the source snapshot.
func TestExportSnapshot_Clone_Isolation(t *testing.T) {
	node := &DiscoveryNode{Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a"}}
	tnode := &TopologyNode{Info: &pb.AtappTopologyInfo{Id: 10}}

	src := &ExportSnapshot{
		Version:     7,
		PublishedAt: time.Now(),
		Discovery: DiscoverySetSnapshot{
			Ready:        true,
			LastRevision: 100,
			NodesByPath:  map[string]*DiscoveryNode{"/svc/a": node},
			NodesByID:    map[uint64]*DiscoveryNode{1: node},
			NodesByName:  map[string]*DiscoveryNode{"svc-a": node},
		},
		Topology: TopologySnapshot{
			Ready:        true,
			LastRevision: 200,
			NodesByID:    map[uint64]*TopologyNode{10: tnode},
		},
	}

	dstDisc := src.Discovery.Clone()
	dstTopo := src.Topology.Clone()

	// --- structural parity ---
	assert.Equal(t, src.Discovery.Ready, dstDisc.Ready)
	assert.Equal(t, src.Discovery.LastRevision, dstDisc.LastRevision)
	assert.Len(t, dstDisc.NodesByPath, 1)
	assert.Len(t, dstDisc.NodesByID, 1)
	assert.Len(t, dstDisc.NodesByName, 1)

	assert.Equal(t, src.Topology.Ready, dstTopo.Ready)
	assert.Equal(t, src.Topology.LastRevision, dstTopo.LastRevision)
	assert.Len(t, dstTopo.NodesByID, 1)

	// --- isolation: adding to clone must not affect source ---
	extra := &DiscoveryNode{Path: "/svc/z", Info: &pb.AtappDiscovery{Id: 99, Name: "svc-z"}}
	dstDisc.NodesByPath["/svc/z"] = extra
	assert.Len(t, src.Discovery.NodesByPath, 1, "source Discovery.NodesByPath must not grow")

	dstTopo.NodesByID[999] = &TopologyNode{Info: &pb.AtappTopologyInfo{Id: 999}}
	assert.Len(t, src.Topology.NodesByID, 1, "source Topology.NodesByID must not grow")
}

// TestExportSnapshot_NilClone verifies Clone on nil sub-snapshots returns empty
// structs rather than panicking.
func TestExportSnapshot_NilClone(t *testing.T) {
	var disc *DiscoverySetSnapshot
	var topo *TopologySnapshot

	assert.NotNil(t, disc.Clone())
	assert.NotNil(t, topo.Clone())
}

// ── DiscoverySetSnapshot – betterNodeForIndex ─────────────────────────────

// TestDiscoverySetSnapshot_BetterNode_ModRevision verifies that a higher
// ModRevision wins over a lower one.
func TestDiscoverySetSnapshot_BetterNode_ModRevision(t *testing.T) {
	old := &DiscoveryNode{
		Path:        "/svc/a",
		DataVersion: etcdversion.DataVersion{ModRevision: 1},
	}
	newer := &DiscoveryNode{
		Path:        "/svc/a",
		DataVersion: etcdversion.DataVersion{ModRevision: 5},
	}

	s := &DiscoverySetSnapshot{}
	s.UpsertNode(old)
	s.NodesByPath["/svc/b"] = newer // inject second path with same id to trigger comparison
	// Direct indexNode comparison:
	assert.True(t, betterNodeForIndex(old, newer))
	assert.False(t, betterNodeForIndex(newer, old))
}

// TestDiscoverySetSnapshot_BetterNode_TiebreakPath verifies that when all
// DataVersion fields are equal the node with the lexicographically smaller
// path wins (deterministic selection).
func TestDiscoverySetSnapshot_BetterNode_TiebreakPath(t *testing.T) {
	a := &DiscoveryNode{Path: "/svc/a", DataVersion: etcdversion.DataVersion{ModRevision: 2}}
	b := &DiscoveryNode{Path: "/svc/b", DataVersion: etcdversion.DataVersion{ModRevision: 2}}

	// "a" < "b" → a is better
	assert.True(t, betterNodeForIndex(b, a))
	assert.False(t, betterNodeForIndex(a, b))
}

// TestDiscoverySetSnapshot_UpsertKeepsHigherRevision verifies that re-upserting
// a node with a lower revision does not overwrite the indexed (higher-revision) entry.
func TestDiscoverySetSnapshot_UpsertKeepsHigherRevision(t *testing.T) {
	s := &DiscoverySetSnapshot{}

	high := &DiscoveryNode{
		Path:        "/svc/a",
		Info:        &pb.AtappDiscovery{Id: 1, Name: "svc"},
		DataVersion: etcdversion.DataVersion{ModRevision: 10},
	}
	low := &DiscoveryNode{
		Path:        "/svc/b", // different path, same id+name
		Info:        &pb.AtappDiscovery{Id: 1, Name: "svc"},
		DataVersion: etcdversion.DataVersion{ModRevision: 2},
	}

	s.UpsertNode(high)
	s.UpsertNode(low)

	// The ID index must still point to the high-revision node.
	got := s.GetNodeByID(1)
	require.NotNil(t, got)
	assert.Equal(t, int64(10), got.ModRevision)
}

// TestDiscoverySetSnapshot_RemoveNonexistentPath is a no-op and must not panic.
func TestDiscoverySetSnapshot_RemoveNonexistentPath(t *testing.T) {
	s := &DiscoverySetSnapshot{}
	assert.NotPanics(t, func() { s.RemoveNodeByPath("/no/such/path") })
}

// TestDiscoverySetSnapshot_RemovePath_RebuildsTiebreaker verifies that after
// removing a path shared by id/name the index falls back to the remaining node.
func TestDiscoverySetSnapshot_RemovePath_RebuildsTiebreaker(t *testing.T) {
	s := &DiscoverySetSnapshot{}

	winner := &DiscoveryNode{
		Path:        "/svc/a",
		Info:        &pb.AtappDiscovery{Id: 1, Name: "svc"},
		DataVersion: etcdversion.DataVersion{ModRevision: 10},
	}
	fallback := &DiscoveryNode{
		Path:        "/svc/b",
		Info:        &pb.AtappDiscovery{Id: 1, Name: "svc"},
		DataVersion: etcdversion.DataVersion{ModRevision: 3},
	}

	s.UpsertNode(winner)
	s.UpsertNode(fallback)
	require.Equal(t, winner, s.GetNodeByID(1))

	// Remove the winner → index must fall back to the remaining node.
	s.RemoveNodeByPath("/svc/a")
	got := s.GetNodeByID(1)
	require.NotNil(t, got)
	assert.Equal(t, "/svc/b", got.Path)
}

// TestDiscoverySetSnapshot_Clone_EmptyMapsAreNil verifies that Clone of a
// snapshot with no nodes produces nil maps (not empty maps).
func TestDiscoverySetSnapshot_Clone_EmptyMapsAreNil(t *testing.T) {
	src := &DiscoverySetSnapshot{Ready: true, LastRevision: 5}
	cloned := src.Clone()
	assert.Nil(t, cloned.NodesByPath)
	assert.Nil(t, cloned.NodesByID)
	assert.Nil(t, cloned.NodesByName)
}

// ── TopologySnapshot – betterNode ─────────────────────────────────────────

// TestTopologySnapshot_BetterNode_ModRevision verifies that a higher ModRevision
// wins.
func TestTopologySnapshot_BetterNode_ModRevision(t *testing.T) {
	old := &TopologyNode{DataVersion: etcdversion.DataVersion{ModRevision: 1}}
	newer := &TopologyNode{DataVersion: etcdversion.DataVersion{ModRevision: 7}}

	assert.True(t, betterNode(old, newer))
	assert.False(t, betterNode(newer, old))
}

// TestTopologySnapshot_BetterNode_EqualVersions verifies that identical
// DataVersions return false (no replacement).
func TestTopologySnapshot_BetterNode_EqualVersions(t *testing.T) {
	a := &TopologyNode{DataVersion: etcdversion.DataVersion{ModRevision: 5, CreateRevision: 1, Version: 2}}
	b := &TopologyNode{DataVersion: etcdversion.DataVersion{ModRevision: 5, CreateRevision: 1, Version: 2}}

	assert.False(t, betterNode(a, b))
	assert.False(t, betterNode(b, a))
}

// TestTopologySnapshot_UpsertZeroID_Ignored verifies that a topology node with
// id=0 is silently ignored (id=0 is an invalid sentinel).
func TestTopologySnapshot_UpsertZeroID_Ignored(t *testing.T) {
	s := &TopologySnapshot{}
	s.UpsertNode(&TopologyNode{Info: &pb.AtappTopologyInfo{Id: 0}})
	assert.Nil(t, s.NodesByID)
}

// TestTopologySnapshot_UpsertKeepsHigherRevision verifies that a lower-revision
// update does not overwrite an existing higher-revision entry.
func TestTopologySnapshot_UpsertKeepsHigherRevision(t *testing.T) {
	s := &TopologySnapshot{}
	high := &TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 1},
		DataVersion: etcdversion.DataVersion{ModRevision: 10},
	}
	low := &TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 1},
		DataVersion: etcdversion.DataVersion{ModRevision: 2},
	}

	s.UpsertNode(high)
	s.UpsertNode(low)

	assert.Equal(t, int64(10), s.GetNodeByID(1).ModRevision)
}

// TestTopologySnapshot_RemoveNodeByID_Unknown is a no-op and must not panic.
func TestTopologySnapshot_RemoveNodeByID_Unknown(t *testing.T) {
	s := &TopologySnapshot{}
	assert.NotPanics(t, func() { s.RemoveNodeByID(999) })
}

// ── SelfRegistrationSnapshot ─────────────────────────────────────────────

// TestSelfRegistrationSnapshot_Clone_Isolation verifies that the clone's maps are
// independent of the source.
func TestSelfRegistrationSnapshot_Clone_Isolation(t *testing.T) {
	disc := &pb.AtappDiscovery{Id: 1, Name: "svc"}
	src := &SelfRegistrationSnapshot{
		LeaseID:    clientv3.LeaseID(7),
		LeaseEpoch: 2,
		ByPath:     map[string]*pb.AtappDiscovery{"/reg/a": disc},
		ByName:     map[string]*pb.AtappDiscovery{"svc": disc},
		ByID:       map[uint64]*pb.AtappDiscovery{1: disc},
		UpdatedAt:  time.Now(),
	}

	clone := src.Clone()

	assert.Equal(t, src.LeaseID, clone.LeaseID)
	assert.Equal(t, src.LeaseEpoch, clone.LeaseEpoch)
	assert.Equal(t, src.UpdatedAt, clone.UpdatedAt)

	// Pointer to the same underlying pb.AtappDiscovery (shallow copy).
	assert.Same(t, src.ByPath["/reg/a"], clone.ByPath["/reg/a"])

	// Mutation of clone must not affect source.
	clone.ByPath["/reg/z"] = &pb.AtappDiscovery{}
	assert.Len(t, src.ByPath, 1)
}

// TestSelfRegistrationSnapshot_Clone_EmptyMapsAreNil verifies nil map semantics.
func TestSelfRegistrationSnapshot_Clone_EmptyMapsAreNil(t *testing.T) {
	src := &SelfRegistrationSnapshot{LeaseID: 5}
	clone := src.Clone()
	assert.Nil(t, clone.ByPath)
	assert.Nil(t, clone.ByName)
	assert.Nil(t, clone.ByID)
}
